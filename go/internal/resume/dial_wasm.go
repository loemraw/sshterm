// MIT License
//
// Copyright (c) 2024 TTBT Enterprises LLC
// Copyright (c) 2024 Robin Thellend <rthellend@rthellend.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//go:build wasm

package resume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall/js"
	"time"

	"github.com/c2FmZQ/sshterm/internal/websocket"
)

const (
	handshakeTimeout = 10 * time.Second
	// silence is how long without any message from the relay before the page
	// counts the WebSocket as dead.
	silence     = 3 * Heartbeat
	resumeQuery = "resume=1"
)

// refusedError is the relay's refusal of a session.
type refusedError struct {
	reason string
}

func (e *refusedError) Error() string {
	return "the relay refused the session: " + e.reason
}

// pageTransport is a Transport over a page WebSocket, with a watchdog that
// closes it after silence.
type pageTransport struct {
	ws       *websocket.WebSocket
	cancelWS context.CancelFunc

	mu        sync.Mutex
	lastHeard time.Time
	closed    bool
	quit      chan struct{}
}

func newPageTransport(ws *websocket.WebSocket, cancelWS context.CancelFunc) *pageTransport {
	t := &pageTransport{
		ws:        ws,
		cancelWS:  cancelWS,
		lastHeard: time.Now(),
		quit:      make(chan struct{}),
	}
	go t.watchdog()
	return t
}

func (t *pageTransport) watchdog() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-t.quit:
			return
		case <-ticker.C:
			t.mu.Lock()
			quiet := time.Since(t.lastHeard)
			t.mu.Unlock()
			if quiet > silence {
				t.Close()
				return
			}
		}
	}
}

func (t *pageTransport) ReadMessage() (bool, []byte, error) {
	text, data, err := t.ws.ReadMessage()
	if err == nil {
		t.mu.Lock()
		t.lastHeard = time.Now()
		t.mu.Unlock()
	}
	return text, data, err
}

func (t *pageTransport) WriteMessage(text bool, data []byte) error {
	return t.ws.WriteMessage(text, data)
}

func (t *pageTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.quit)
	t.mu.Unlock()
	err := t.ws.Close()
	t.cancelWS()
	return err
}

// handshake opens a WebSocket to url, sends hello and waits for the welcome.
func handshake(ctx context.Context, url string, hello Message) (Transport, Message, error) {
	// websocket.New keeps its context for the connection's whole life, so the
	// timeout only cancels it during the handshake.
	wsCtx, cancelWS := context.WithCancel(ctx)
	timer := time.AfterFunc(handshakeTimeout, cancelWS)
	ws, err := websocket.New(wsCtx, url, io.Discard)
	if err != nil {
		timer.Stop()
		cancelWS()
		return nil, Message{}, err
	}
	t := newPageTransport(ws, cancelWS)
	if err := WriteJSON(t, hello); err != nil {
		timer.Stop()
		t.Close()
		return nil, Message{}, err
	}
	m, err := ReadJSON(t)
	timer.Stop()
	if err != nil {
		t.Close()
		return nil, Message{}, err
	}
	switch {
	case m.Type == "error":
		t.Close()
		return nil, Message{}, &refusedError{reason: m.Error}
	case m.Type != "welcome":
		t.Close()
		return nil, Message{}, &refusedError{reason: fmt.Sprintf("unexpected %q message instead of welcome", m.Type)}
	case hello.Session != "" && m.Session != hello.Session:
		t.Close()
		return nil, Message{}, &refusedError{reason: "the relay answered for a different session"}
	}
	return t, m, nil
}

// Dial connects to the relay endpoint at url and returns a connection to its SSH
// server that outlives the WebSocket: when the WebSocket drops, Dial's connection
// reconnects and resumes the session, and SSH doesn't notice. notify shows a short
// message to the user, e.g. Terminal.ShowMessage.
//
// The page sends no SSH keepalives on such a connection. A relay that stops
// answering is a dropped connection, which Dial resumes; the session ends only
// when the relay refuses it, when ctx ends, or after Grace without the relay.
func Dial(ctx context.Context, url string, notify func(format string, args ...any)) (net.Conn, error) {
	if strings.Contains(url, "?") {
		url += "&" + resumeQuery
	} else {
		url += "?" + resumeQuery
	}
	t, w, err := handshake(ctx, url, Message{Type: "hello"})
	if err != nil {
		return nil, err
	}
	stream := NewStream()
	done, err := stream.Attach(t, w.Received)
	if err != nil {
		t.Close()
		return nil, err
	}
	id := w.Session

	go keep(ctx, url, id, stream, done, notify)

	pagehide := js.FuncOf(func(this js.Value, args []js.Value) any {
		stream.Close(nil)
		return nil
	})
	js.Global().Call("addEventListener", "pagehide", pagehide)
	go func() {
		<-stream.Done()
		js.Global().Call("removeEventListener", "pagehide", pagehide)
		pagehide.Release()
	}()

	return &conn{stream}, nil
}

// keep reattaches the stream to a new transport whenever the current one stops.
func keep(ctx context.Context, url, id string, stream *Stream, done <-chan error, notify func(format string, args ...any)) {
	for {
		// Wait for the transport to stop, for whatever reason.
		<-done
		select {
		case <-stream.Done():
			return
		default:
		}
		notify("Connection lost, reconnecting…")
		lostAt := time.Now()

		for attempt := 0; ; attempt++ {
			delay := 5 * time.Second
			if attempt < 4 {
				delay = []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second}[attempt]
			}
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				stream.Close(ctx.Err())
				return
			case <-stream.Done():
				timer.Stop()
				return
			}
			if time.Since(lostAt) > Grace {
				stream.Close(fmt.Errorf("the connection was lost for over %v", Grace))
				return
			}
			stream.Detach()
			t, w, err := handshake(ctx, url, Message{Type: "hello", Session: id, Received: stream.Received()})
			if err != nil {
				var refused *refusedError
				if errors.As(err, &refused) {
					stream.Close(err)
					return
				}
				continue
			}
			done, err = stream.Attach(t, w.Received)
			if err != nil {
				t.Close()
				stream.Close(err)
				return
			}
			notify("Reconnected")
			break
		}
	}
}

// conn is a net.Conn over a Stream.
type conn struct {
	stream *Stream
}

func (c *conn) Read(b []byte) (int, error)  { return c.stream.Read(b) }
func (c *conn) Write(b []byte) (int, error) { return c.stream.Write(b) }
func (c *conn) Close() error {
	c.stream.Close(nil)
	return nil
}
func (c *conn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *conn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *conn) SetDeadline(t time.Time) error      { return nil }
func (c *conn) SetReadDeadline(t time.Time) error  { return nil }
func (c *conn) SetWriteDeadline(t time.Time) error { return nil }

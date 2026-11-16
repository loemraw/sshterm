// MIT License
//
// Copyright (c) 2026 TTBT Enterprises LLC
// Copyright (c) 2026 Robin Thellend <rthellend@rthellend.com>
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

package internal

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/c2FmZQ/sshterm/internal/resume"
)

type clientTransport struct {
	c *websocket.Conn
}

func (t *clientTransport) ReadMessage() (bool, []byte, error) {
	typ, data, err := t.c.ReadMessage()
	return typ == websocket.TextMessage, data, err
}

func (t *clientTransport) WriteMessage(text bool, data []byte) error {
	typ := websocket.BinaryMessage
	if text {
		typ = websocket.TextMessage
	}
	return t.c.WriteMessage(typ, data)
}

func (t *clientTransport) Close() error { return t.c.Close() }

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type resumeEnv struct {
	srv   *Server
	wsURL string
	// upstream receives each connection the fake upstream accepts.
	upstream chan net.Conn
}

// newResumeEnv starts a server with an endpoint "up" whose upstream is handler,
// which runs for each accepted connection.
func newResumeEnv(t *testing.T, logger *log.Logger, handler func(net.Conn)) *resumeEnv {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	env := &resumeEnv{upstream: make(chan net.Conn, 16)}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			env.upstream <- c
			go func() {
				defer c.Close()
				handler(c)
			}()
		}
	}()
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	srv, err := New(Options{
		Endpoints: []Endpoint{{Name: "up", Address: l.Addr().String()}},
		Logger:    logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	env.srv = srv
	env.wsURL = "ws" + strings.TrimPrefix(ts.URL, "http") + "/_ws/up?resume=1"
	return env
}

func echo(c net.Conn) { _, _ = io.Copy(c, c) }

func (e *resumeEnv) dial(t *testing.T) (*clientTransport, *websocket.Conn) {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(e.wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &clientTransport{c: c}, c
}

// connect sends a hello and returns the reply.
func (e *resumeEnv) connect(t *testing.T, session string, received uint64) (*clientTransport, *websocket.Conn, resume.Message) {
	t.Helper()
	tr, c := e.dial(t)
	if err := resume.WriteJSON(tr, resume.Message{Type: "hello", Session: session, Received: received}); err != nil {
		t.Fatal(err)
	}
	m, err := resume.ReadJSON(tr)
	if err != nil {
		t.Fatal(err)
	}
	return tr, c, m
}

// start opens a new session and attaches page to it.
func (e *resumeEnv) start(t *testing.T, page *resume.Stream) (string, *websocket.Conn, <-chan error) {
	t.Helper()
	tr, c, m := e.connect(t, "", 0)
	if m.Type != "welcome" {
		t.Fatalf("reply = %+v, want welcome", m)
	}
	done, err := page.Attach(tr, m.Received)
	if err != nil {
		t.Fatal(err)
	}
	return m.Session, c, done
}

func (e *resumeEnv) sessionCount() int {
	e.srv.sessMu.Lock()
	defer e.srv.sessMu.Unlock()
	return len(e.srv.sessions)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitDone(t *testing.T, what string, ch <-chan error) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func pattern(start, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((start + i) % 251)
	}
	return b
}

func readFull(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	errc := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(r, buf)
		errc <- err
	}()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out reading")
	}
	return buf
}

func TestResumableSessionEchoes(t *testing.T) {
	env := newResumeEnv(t, nil, echo)
	page := resume.NewStream()
	defer page.Close(nil)
	tr, _, m := env.connect(t, "", 0)
	if _, err := page.Attach(tr, m.Received); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(m.Session) {
		t.Errorf("session id = %q, want 32 hex characters", m.Session)
	}
	if m.Type != "welcome" || m.Received != 0 {
		t.Errorf("welcome = %+v", m)
	}
	msg := []byte("hello, echo")
	if _, err := page.Write(msg); err != nil {
		t.Fatal(err)
	}
	if got := readFull(t, page, len(msg)); !bytes.Equal(got, msg) {
		t.Errorf("got %q, want %q", got, msg)
	}
}

func TestResumableSessionSurvivesDroppedConnection(t *testing.T) {
	env := newResumeEnv(t, nil, echo)
	page := resume.NewStream()
	defer page.Close(nil)
	id, conn, done := env.start(t, page)

	const total = 600 << 10
	data := pattern(0, total)
	var got []byte
	var gotMu sync.Mutex
	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 8192)
		for {
			n, err := page.Read(buf)
			gotMu.Lock()
			got = append(got, buf[:n]...)
			count := len(got)
			gotMu.Unlock()
			if err != nil || count >= total {
				readErr <- err
				return
			}
		}
	}()

	first := 100 << 10
	if _, err := page.Write(data[:first]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "some echoed data", func() bool {
		gotMu.Lock()
		defer gotMu.Unlock()
		return len(got) > 0
	})

	// Kill the connection with no close frame.
	_ = conn.UnderlyingConn().Close()
	waitDone(t, "first transport to stop", done)
	page.Detach()

	// Written while detached.
	if _, err := page.Write(data[first : first+200<<10]); err != nil {
		t.Fatal(err)
	}

	tr, _, m := env.connect(t, id, page.Received())
	if m.Type != "welcome" || m.Session != id {
		t.Fatalf("reply = %+v", m)
	}
	if _, err := page.Attach(tr, m.Received); err != nil {
		t.Fatal(err)
	}
	if _, err := page.Write(data[first+200<<10:]); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-readErr:
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for echo")
	}
	gotMu.Lock()
	defer gotMu.Unlock()
	if !bytes.Equal(got, data) {
		t.Errorf("echoed %d bytes, not equal to the %d sent", len(got), len(data))
	}
}

func TestResumeTakesOverAnOpenConnection(t *testing.T) {
	env := newResumeEnv(t, nil, echo)
	page := resume.NewStream()
	defer page.Close(nil)
	id, _, done := env.start(t, page)

	if _, err := page.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if got := readFull(t, page, 3); string(got) != "one" {
		t.Fatalf("got %q", got)
	}

	// Resume with the first WebSocket still open. A separate page stream
	// state is not needed: the same stream moves to the new transport.
	tr, _, m := env.connect(t, id, page.Received())
	if m.Type != "welcome" {
		t.Fatalf("reply = %+v", m)
	}
	waitDone(t, "first transport to stop", done)
	if _, err := page.Attach(tr, m.Received); err != nil {
		t.Fatal(err)
	}
	if _, err := page.Write([]byte("two")); err != nil {
		t.Fatal(err)
	}
	if got := readFull(t, page, 3); string(got) != "two" {
		t.Fatalf("got %q", got)
	}
}

func TestResumeOfUnknownSessionIsRefused(t *testing.T) {
	env := newResumeEnv(t, nil, echo)
	_, _, m := env.connect(t, strings.Repeat("ab", 16), 0)
	if m.Type != "error" || m.Error != "unknown session" {
		t.Errorf("reply = %+v, want error: unknown session", m)
	}
}

func TestByeEndsSession(t *testing.T) {
	sawEOF := make(chan error, 1)
	env := newResumeEnv(t, nil, func(c net.Conn) {
		_, err := io.Copy(io.Discard, c)
		sawEOF <- err
	})
	page := resume.NewStream()
	env.start(t, page)
	waitFor(t, "the session", func() bool { return env.sessionCount() == 1 })

	page.Close(nil)

	select {
	case err := <-sawEOF:
		if err != nil {
			t.Errorf("upstream read ended with %v, want EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream EOF")
	}
	waitFor(t, "the session to end", func() bool { return env.sessionCount() == 0 })
}

func TestSessionEndsAfterGrace(t *testing.T) {
	env := newResumeEnv(t, nil, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
	})
	env.srv.grace = 200 * time.Millisecond
	page := resume.NewStream()
	defer page.Close(nil)
	_, conn, done := env.start(t, page)
	waitFor(t, "the session", func() bool { return env.sessionCount() == 1 })

	_ = conn.UnderlyingConn().Close()
	waitDone(t, "transport to stop", done)

	waitFor(t, "the session to end", func() bool { return env.sessionCount() == 0 })
	up := <-env.upstream
	_ = up.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := up.Read(make([]byte, 1)); err == nil {
		t.Error("upstream read succeeded, want the connection closed")
	}
}

func TestUpstreamCloseReachesPage(t *testing.T) {
	env := newResumeEnv(t, nil, func(c net.Conn) {
		_, _ = c.Write([]byte("goodbye"))
	})
	page := resume.NewStream()
	defer page.Close(nil)
	env.start(t, page)

	if got := readFull(t, page, 7); string(got) != "goodbye" {
		t.Fatalf("got %q", got)
	}
	errc := make(chan error, 1)
	go func() {
		_, err := page.Read(make([]byte, 1))
		errc <- err
	}()
	select {
	case err := <-errc:
		if err != io.EOF {
			t.Errorf("read error = %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for EOF")
	}
}

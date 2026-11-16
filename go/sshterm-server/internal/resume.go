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
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/c2FmZQ/sshterm/internal/resume"
)

// wsTransport is a resume.Transport over a WebSocket connection.
type wsTransport struct {
	c        *websocket.Conn
	quit     chan struct{}
	stopOnce sync.Once
}

func newWSTransport(c *websocket.Conn) *wsTransport {
	t := &wsTransport{c: c, quit: make(chan struct{})}
	_ = c.SetReadDeadline(time.Now().Add(dialTimeout))
	c.SetPongHandler(func(string) error {
		return c.SetReadDeadline(time.Now().Add(pongWait))
	})
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-t.quit:
				return
			case <-ticker.C:
				if err := c.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
					t.Close()
					return
				}
			}
		}
	}()
	return t
}

func (t *wsTransport) ReadMessage() (bool, []byte, error) {
	typ, data, err := t.c.ReadMessage()
	if err != nil {
		return false, nil, err
	}
	_ = t.c.SetReadDeadline(time.Now().Add(pongWait))
	return typ == websocket.TextMessage, data, nil
}

func (t *wsTransport) WriteMessage(text bool, data []byte) error {
	_ = t.c.SetWriteDeadline(time.Now().Add(writeWait))
	typ := websocket.BinaryMessage
	if text {
		typ = websocket.TextMessage
	}
	return t.c.WriteMessage(typ, data)
}

func (t *wsTransport) Close() error {
	t.stopOnce.Do(func() {
		close(t.quit)
		_ = t.c.Close()
	})
	return nil
}

// session is an SSH connection that outlives the WebSocket carrying it.
type session struct {
	id     string
	stream *resume.Stream
	tcp    net.Conn

	mu        sync.Mutex  // serializes takeovers and guards the fields below
	gen       int         // counts attachments, so a stale one doesn't start the grace timer
	grace     *time.Timer // runs while no page is attached
	droppedAt time.Time   // when the last page dropped; zero while one is attached
}

// label is what logs call the session. The full id resumes it, so it is never
// logged.
func (sess *session) label() string {
	return sess.id[:8]
}

func shortLabel(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// startGrace starts the timer that ends the session if no page comes back.
// Call it with sess.mu held.
func (s *Server) startGrace(sess *session) {
	sess.grace = time.AfterFunc(s.grace, func() {
		sess.stream.Close(fmt.Errorf("no page for %v", s.grace))
	})
}

// serveResumable serves a WebSocket that speaks the resume protocol, until
// that WebSocket is done. The session may outlive it.
func (s *Server) serveResumable(wsConn *websocket.Conn, name, targetAddr string) {
	t := newWSTransport(wsConn)
	hello, err := resume.ReadJSON(t)
	if err == nil && hello.Type != "hello" {
		err = fmt.Errorf("got %q", hello.Type)
	}
	if err != nil {
		s.logger.Printf("resume %q: no hello: %v", name, err)
		t.Close()
		return
	}
	refuse := func(msg string) {
		_ = resume.WriteJSON(t, resume.Message{Type: "error", Error: msg})
		t.Close()
	}

	var sess *session
	resumed := hello.Session != ""
	if !resumed {
		if hello.Received != 0 {
			refuse("a new session has received nothing")
			return
		}
		dialCtx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		tcpConn, err := s.opts.DialContext(dialCtx, "tcp", targetAddr)
		cancel()
		if err != nil {
			s.logger.Printf("dial %q (%s) failed: %v", name, targetAddr, err)
			refuse("upstream connection failed")
			return
		}
		sess = s.newSession(tcpConn)
		s.logger.Printf("session %s: new, to %q", sess.label(), name)
	} else {
		s.sessMu.Lock()
		sess = s.sessions[hello.Session]
		s.sessMu.Unlock()
		if sess == nil {
			s.logger.Printf("session %s: unknown, can't resume", shortLabel(hello.Session))
			refuse("unknown session")
			return
		}
	}

	sess.mu.Lock()
	sess.stream.Detach()
	if sess.grace != nil {
		sess.grace.Stop()
		sess.grace = nil
	}
	err = resume.WriteJSON(t, resume.Message{Type: "welcome", Session: sess.id, Received: sess.stream.Received()})
	if err != nil {
		t.Close()
		s.startGrace(sess)
		sess.mu.Unlock()
		return
	}
	done, err := sess.stream.Attach(t, hello.Received)
	if err != nil {
		_ = resume.WriteJSON(t, resume.Message{Type: "error", Error: err.Error()})
		t.Close()
		sess.mu.Unlock()
		sess.stream.Close(err)
		return
	}
	sess.gen++
	gen := sess.gen
	if resumed {
		resending := sess.stream.Sent() - hello.Received
		if sess.droppedAt.IsZero() {
			// The page came back before its old connection was seen to drop.
			s.logger.Printf("session %s: resumed, replacing a connection that hadn't dropped yet, resending %d bytes", sess.label(), resending)
		} else {
			away := time.Since(sess.droppedAt).Round(time.Second)
			s.logger.Printf("session %s: resumed after %v, resending %d bytes", sess.label(), away, resending)
		}
	}
	// Zero while a page is attached.
	sess.droppedAt = time.Time{}
	sess.mu.Unlock()

	reason := <-done

	sess.mu.Lock()
	defer sess.mu.Unlock()
	select {
	case <-sess.stream.Done():
	default:
		if sess.gen == gen {
			sess.droppedAt = time.Now()
			s.startGrace(sess)
			s.logger.Printf("session %s: page dropped: %v", sess.label(), reason)
		}
	}
}

// newSession registers a session for tcpConn and starts its goroutines.
func (s *Server) newSession(tcpConn net.Conn) *session {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	sess := &session{
		id:     hex.EncodeToString(b[:]),
		stream: resume.NewStream(),
		tcp:    tcpConn,
	}
	s.sessMu.Lock()
	s.sessions[sess.id] = sess
	s.sessMu.Unlock()

	// TCP -> stream
	go func() {
		buf := make([]byte, 16384)
		for {
			n, err := tcpConn.Read(buf)
			if n > 0 {
				if _, wErr := sess.stream.Write(buf[:n]); wErr != nil {
					return
				}
			}
			if err != nil {
				sess.stream.CloseWrite()
				return
			}
		}
	}()

	// stream -> TCP
	go func() {
		w := &tcpDeadlineWriter{conn: tcpConn}
		buf := make([]byte, 16384)
		for {
			n, err := sess.stream.Read(buf)
			if n > 0 {
				if _, wErr := w.Write(buf[:n]); wErr != nil {
					sess.stream.Close(wErr)
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// Cleanup
	go func() {
		<-sess.stream.Done()
		s.sessMu.Lock()
		delete(s.sessions, sess.id)
		s.sessMu.Unlock()
		sess.mu.Lock()
		if sess.grace != nil {
			sess.grace.Stop()
		}
		sess.mu.Unlock()
		tcpConn.Close()
		s.logger.Printf("session %s: ended: %v", sess.label(), sess.stream.Err())
	}()
	return sess
}

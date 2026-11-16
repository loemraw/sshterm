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

package resume

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// link is the shared state of two connected in-memory transports.
type link struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queues [2][]message
	closed bool
	// limit, if not zero, is how many messages the link carries before it
	// cuts itself.
	limit   int
	written int
}

type message struct {
	text bool
	data []byte
}

// end is one end of a link.
type end struct {
	l    *link
	side int
}

// pipe returns two connected Transports.
func pipe() (*end, *end) {
	l := &link{}
	l.cond = sync.NewCond(&l.mu)
	return &end{l, 0}, &end{l, 1}
}

// cutAfter makes the link cut itself after n messages.
func (e *end) cutAfter(n int) {
	e.l.mu.Lock()
	defer e.l.mu.Unlock()
	e.l.limit = n
}

// cut silently drops the messages in flight, and makes both ends fail.
func (e *end) cut() {
	e.l.mu.Lock()
	defer e.l.mu.Unlock()
	e.l.closed = true
	e.l.queues = [2][]message{}
	e.l.cond.Broadcast()
}

func (e *end) ReadMessage() (bool, []byte, error) {
	l := e.l
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.queues[e.side]) == 0 && !l.closed {
		l.cond.Wait()
	}
	if len(l.queues[e.side]) == 0 {
		return false, nil, io.ErrClosedPipe
	}
	m := l.queues[e.side][0]
	l.queues[e.side] = l.queues[e.side][1:]
	return m.text, m.data, nil
}

func (e *end) WriteMessage(text bool, data []byte) error {
	l := e.l
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return io.ErrClosedPipe
	}
	l.written++
	if l.limit > 0 && l.written > l.limit {
		l.closed = true
		l.queues = [2][]message{}
		l.cond.Broadcast()
		return io.ErrClosedPipe
	}
	l.queues[1-e.side] = append(l.queues[1-e.side], message{text, append([]byte(nil), data...)})
	l.cond.Broadcast()
	return nil
}

// Close is a graceful close: messages already written can still be read by
// the other end, like with a WebSocket close handshake. Then both ends fail.
func (e *end) Close() error {
	e.l.mu.Lock()
	defer e.l.mu.Unlock()
	e.l.closed = true
	e.l.cond.Broadcast()
	return nil
}

// counter wraps a Transport and counts the text messages written to it.
type counter struct {
	Transport
	mu    sync.Mutex
	texts int
}

func (c *counter) WriteMessage(text bool, data []byte) error {
	if text {
		c.mu.Lock()
		c.texts++
		c.mu.Unlock()
	}
	return c.Transport.WriteMessage(text, data)
}

func (c *counter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.texts
}

// connect attaches a and b to the two ends of a new pipe, each telling the
// other's Received count.
func connect(t *testing.T, a, b *Stream) (*end, <-chan error, <-chan error) {
	t.Helper()
	ta, tb := pipe()
	doneA, err := a.Attach(ta, b.Received())
	if err != nil {
		t.Fatalf("a.Attach: %v", err)
	}
	doneB, err := b.Attach(tb, a.Received())
	if err != nil {
		t.Fatalf("b.Attach: %v", err)
	}
	return ta, doneA, doneB
}

// waitFor polls cond until it is true, or fails the test after 5 seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitChan[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// pattern returns the byte at offset i of a stream where a gap or a duplicate
// shows up as a mismatch.
func pattern(i int) byte {
	return byte((uint32(i) * 2654435761) >> 24)
}

func patternBytes(start, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = pattern(start + i)
	}
	return b
}

// readAllAsync reads exactly n bytes from s in the background.
func readAllAsync(s *Stream, n int) <-chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		buf := make([]byte, n)
		if _, err := io.ReadFull(s, buf); err != nil {
			buf = nil
		}
		ch <- buf
	}()
	return ch
}

func writeAll(t *testing.T, s *Stream, b []byte) {
	t.Helper()
	if n, err := s.Write(b); err != nil || n != len(b) {
		t.Errorf("Write = %d, %v; want %d, nil", n, err, len(b))
	}
}

func TestStreamCarriesBytesBothWays(t *testing.T) {
	a, b := NewStream(), NewStream()
	connect(t, a, b)

	const big = 3 << 20
	fromA := patternBytes(0, big)
	fromB := patternBytes(12345, 100000)
	gotB := readAllAsync(b, len(fromA))
	gotA := readAllAsync(a, len(fromB))
	go writeAll(t, a, fromA)
	go writeAll(t, b, fromB)

	if got := waitChan(t, "b to read", gotB); !bytes.Equal(got, fromA) {
		t.Error("b did not read what a wrote")
	}
	if got := waitChan(t, "a to read", gotA); !bytes.Equal(got, fromB) {
		t.Error("a did not read what b wrote")
	}
}

func TestStreamResumesWithoutLossOrDuplicates(t *testing.T) {
	a, b := NewStream(), NewStream()
	const total = 3 << 20
	got := readAllAsync(b, total)
	go func() {
		for off := 0; off < total; off += 1000 {
			writeAll(t, a, patternBytes(off, min(1000, total-off)))
		}
	}()

	cuts := 0
	for {
		a.Detach()
		b.Detach()
		ta, tb := pipe()
		ta.cutAfter(40) // Messages in both directions count.
		doneA, err := a.Attach(ta, b.Received())
		if err != nil {
			t.Fatalf("a.Attach: %v", err)
		}
		doneB, err := b.Attach(tb, a.Received())
		if err != nil {
			t.Fatalf("b.Attach: %v", err)
		}
		cuts++
		select {
		case buf := <-got:
			if !bytes.Equal(buf, patternBytes(0, total)) {
				t.Fatal("b read something other than what a wrote")
			}
			t.Logf("link cut %d times", cuts)
			return
		case <-doneA:
			waitChan(t, "b's transport to stop", doneB)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the link to be cut")
		}
	}
}

func TestStreamRejectsImpossiblePeerCount(t *testing.T) {
	a, b := NewStream(), NewStream()
	connect(t, a, b)
	writeAll(t, a, patternBytes(0, 100))
	if got := waitChan(t, "b to read", readAllAsync(b, 100)); got == nil {
		t.Fatal("b failed to read")
	}
	waitFor(t, "a's bytes to be acknowledged", func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return len(a.unacked) == 0
	})
	a.Detach()
	b.Detach()

	for _, n := range []uint64{a.Sent() + 1, 50} {
		ta, _ := pipe()
		if done, err := a.Attach(ta, n); err == nil {
			t.Errorf("Attach(%d) = %v, want an error", n, done)
		}
	}
	ta, _ := pipe()
	if _, err := a.Attach(ta, a.Sent()); err != nil {
		t.Errorf("Attach(%d) = %v, want nil", a.Sent(), err)
	}
}

func TestStreamWriteWaitsForAcks(t *testing.T) {
	a, b := NewStream(), NewStream()
	writeAll(t, a, make([]byte, MaxUnacked))

	returned := make(chan struct{})
	go func() {
		writeAll(t, a, []byte{1})
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("Write returned with no room and no transport")
	case <-time.After(100 * time.Millisecond):
	}

	go io.Copy(io.Discard, b)
	connect(t, a, b)
	waitChan(t, "Write to complete", returned)
}

func TestStreamEndReachesPeerAfterData(t *testing.T) {
	t.Run("on one connection", func(t *testing.T) {
		a, b := NewStream(), NewStream()
		connect(t, a, b)
		data := patternBytes(0, 50000)
		writeAll(t, a, data)
		a.CloseWrite()

		got := make(chan []byte, 1)
		go func() {
			buf, err := io.ReadAll(b)
			if err != nil {
				buf = nil
			}
			got <- buf
		}()
		if buf := waitChan(t, "b to reach EOF", got); !bytes.Equal(buf, data) {
			t.Errorf("b read %d bytes, want %d", len(buf), len(data))
		}
		if _, err := a.Write([]byte{1}); !errors.Is(err, ErrClosed) {
			t.Errorf("Write after CloseWrite = %v, want ErrClosed", err)
		}
	})

	t.Run("after a resume", func(t *testing.T) {
		a, b := NewStream(), NewStream()
		ta, doneA, doneB := connect(t, a, b)
		data := patternBytes(0, 50000)
		writeAll(t, a, data)
		a.CloseWrite()
		got := make(chan []byte, 1)
		go func() {
			buf, err := io.ReadAll(b)
			if err != nil {
				buf = nil
			}
			got <- buf
		}()

		ta.cut()
		waitChan(t, "a's transport to stop", doneA)
		waitChan(t, "b's transport to stop", doneB)
		a.Detach()
		b.Detach()
		connect(t, a, b)
		if buf := waitChan(t, "b to reach EOF", got); !bytes.Equal(buf, data) {
			t.Errorf("b read %d bytes, want %d", len(buf), len(data))
		}
	})
}

func TestStreamByeClosesPeer(t *testing.T) {
	a, b := NewStream(), NewStream()
	connect(t, a, b)
	writeAll(t, a, []byte("hi"))
	waitFor(t, "b to receive the data", func() bool { return b.Received() == 2 })

	a.Close(nil)
	waitChan(t, "b to close", b.Done())
	if err := b.Err(); err != ErrPeerClosed {
		t.Errorf("b.Err() = %v, want ErrPeerClosed", err)
	}
	if err := a.Err(); err != ErrClosed {
		t.Errorf("a.Err() = %v, want ErrClosed", err)
	}
	buf := make([]byte, 10)
	if n, err := b.Read(buf); n != 2 || err != nil {
		t.Errorf("Read = %d, %v; want the 2 buffered bytes", n, err)
	}
	if n, err := b.Read(buf); n != 0 || err != ErrPeerClosed {
		t.Errorf("Read = %d, %v; want 0, ErrPeerClosed", n, err)
	}
	if _, err := b.Attach(&end{}, 0); err != ErrPeerClosed {
		t.Errorf("Attach on a closed stream = %v, want ErrPeerClosed", err)
	}
}

func TestStreamDetachStopsOldTransport(t *testing.T) {
	a := NewStream()
	t1, _ := pipe()
	done1, err := a.Attach(t1, 0)
	if err != nil {
		t.Fatal(err)
	}
	t2, _ := pipe()
	if _, err := a.Attach(t2, 0); err != nil {
		t.Fatal(err)
	}
	if err := waitChan(t, "the first transport to stop", done1); err != ErrReplaced {
		t.Errorf("first transport stopped with %v, want ErrReplaced", err)
	}
	if err := t1.WriteMessage(false, nil); err == nil {
		t.Error("the first transport is still open")
	}
	if err := t2.WriteMessage(false, nil); err != nil {
		t.Errorf("the second transport failed: %v", err)
	}
}

func TestStreamHeartbeatAcksWhenIdle(t *testing.T) {
	a, b := NewStream(), NewStream()
	a.heartbeat = 20 * time.Millisecond
	b.heartbeat = 20 * time.Millisecond

	ta, tb := pipe()
	ca := &counter{Transport: ta}
	if _, err := a.Attach(ca, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Attach(tb, 0); err != nil {
		t.Fatal(err)
	}
	// The first ack is sent on attach. More mean heartbeats.
	waitFor(t, "heartbeat acks", func() bool { return ca.count() >= 5 })
}

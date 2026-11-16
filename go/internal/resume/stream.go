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

// Package resume provides a byte stream that survives switching from one
// WebSocket connection (a "transport") to the next.
//
// Each side keeps the bytes it sent until the other side acknowledges them.
// After a reconnect, each side tells the other how many bytes it has received,
// and the sender resends whatever the peer is missing. The design follows
// Eternal Terminal.
//
// # Wire protocol
//
// Data travels in binary WebSocket messages: raw stream bytes, at most 16 KiB
// per message.
//
// Control messages travel in text messages, as JSON for Message. The types are:
//
//   - hello: page to relay, the first message. Its Session is empty for a new
//     session.
//   - welcome: relay to page, the reply to hello.
//   - error: relay to page, sent instead of a welcome.
//   - ack: both directions. Received is the total number of data bytes received
//     so far.
//   - end: the sender will send no more data.
//   - bye: the sender closed the session for good.
//
// The callers handle the handshake types (hello, welcome and error). A Stream
// sends and handles only ack, end and bye.
package resume

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	// MaxUnacked is how many sent bytes a side holds for the peer to
	// acknowledge. Write waits beyond it.
	MaxUnacked = 1 << 20
	// Heartbeat is how often each side sends an ack, even with nothing new to
	// acknowledge.
	Heartbeat = 5 * time.Second
	// Grace is how long a session outlives its connection: the relay keeps it
	// this long for the page to come back.
	Grace = 30 * time.Minute

	// maxMessage is the largest data message.
	maxMessage = 16 << 10
)

var (
	// ErrClosed is returned when the session was closed on this side, or after
	// CloseWrite.
	ErrClosed = errors.New("resume: the session is closed")
	// ErrPeerClosed is the error of a session that the other side closed for
	// good.
	ErrPeerClosed = errors.New("resume: the other side closed the session")
	// ErrReplaced is the reason a transport stopped when a newer one took
	// over, or when it was detached.
	ErrReplaced = errors.New("resume: a newer connection took over")
)

// Message is a control message, sent as JSON in a text message.
type Message struct {
	Type     string `json:"type"`
	Session  string `json:"session,omitempty"`
	Received uint64 `json:"received"`
	Error    string `json:"error,omitempty"`
}

// Transport is one WebSocket connection.
type Transport interface {
	// ReadMessage returns the next message, and whether it's a text message.
	ReadMessage() (text bool, data []byte, err error)
	// WriteMessage sends one message. A Stream never calls it concurrently.
	WriteMessage(text bool, data []byte) error
	// Close closes the connection and makes ReadMessage return an error.
	Close() error
}

// WriteJSON sends m as a text message on t.
func WriteJSON(t Transport, m Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return t.WriteMessage(true, b)
}

// ReadJSON reads one control message from t. A binary message is an error.
func ReadJSON(t Transport) (Message, error) {
	text, data, err := t.ReadMessage()
	if err != nil {
		return Message{}, err
	}
	if !text {
		return Message{}, fmt.Errorf("resume: expected a control message")
	}
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return Message{}, fmt.Errorf("resume: bad control message: %w", err)
	}
	return m, nil
}

// attachment is one transport attached to a Stream, with its goroutines.
// Its fields are protected by the Stream's mu.
type attachment struct {
	t          Transport
	nextToSend uint64 // the stream offset the next data message starts at
	ackDue     bool
	endSent    bool
	stopped    bool
	reason     error
	wg         sync.WaitGroup // the reader, sender and heartbeat goroutines
	quit       chan struct{}  // closed when the attachment stops
	done       chan error     // receives the reason once the goroutines exit
}

// Stream is a reliable byte stream that outlives its transports. It is an
// io.ReadWriter for the layer above (SSH). Attach connects it to a transport,
// and Detach or a transport failure disconnects it. While detached, Write
// buffers up to MaxUnacked bytes and Read returns what already arrived.
type Stream struct {
	mu   sync.Mutex
	cond *sync.Cond

	// writeMu is held around every call to a transport's WriteMessage,
	// because only one writer at a time is allowed.
	writeMu sync.Mutex

	sent      uint64 // total bytes ever passed to Write
	unacked   []byte // the last len(unacked) of those bytes, not yet acknowledged
	received  uint64 // total data bytes received from the peer
	readBuf   []byte // received, not yet returned by Read
	ended     bool   // CloseWrite was called
	peerEnded bool   // an end message arrived
	closed    bool
	closeErr  error
	done      chan struct{}
	cur       *attachment // nil while detached

	heartbeat time.Duration
}

// NewStream returns a detached Stream.
func NewStream() *Stream {
	s := &Stream{
		done:      make(chan struct{}),
		heartbeat: Heartbeat,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// acked returns how many sent bytes the peer has acknowledged. Call it with
// mu held.
func (s *Stream) acked() uint64 {
	return s.sent - uint64(len(s.unacked))
}

// Read returns bytes sent by the peer. Buffered data is returned first, even
// after the stream closed or the peer ended. After that, it returns the close
// error, or io.EOF if the peer called CloseWrite.
func (s *Stream) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.readBuf) == 0 && !s.peerEnded && !s.closed {
		s.cond.Wait()
	}
	if len(s.readBuf) > 0 {
		n := copy(p, s.readBuf)
		s.readBuf = s.readBuf[n:]
		return n, nil
	}
	if s.closed {
		return 0, s.closeErr
	}
	return 0, io.EOF
}

// Write queues p to be sent. It keeps the bytes until the peer acknowledges
// them, so they survive a change of transport. It never touches a transport,
// and it waits only while MaxUnacked bytes are waiting for acknowledgement.
func (s *Stream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, s.closeErr
	}
	if s.ended {
		return 0, ErrClosed
	}
	for len(s.unacked) > 0 && len(s.unacked)+len(p) > MaxUnacked {
		s.cond.Wait()
		if s.closed {
			return 0, s.closeErr
		}
		if s.ended {
			return 0, ErrClosed
		}
	}
	s.unacked = append(s.unacked, p...)
	s.sent += uint64(len(p))
	s.cond.Broadcast()
	return len(p), nil
}

// CloseWrite tells the peer that no more data will be sent. The end message
// follows the last data byte, on every transport attached from now on, so a
// resumed peer gets it too.
func (s *Stream) CloseWrite() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = true
	s.cond.Broadcast()
}

// Close closes the session for good, and tells the peer with a bye message if
// a transport is attached. A nil err means ErrClosed. Only the first call
// counts.
func (s *Stream) Close(err error) {
	s.closeWith(err, true)
}

// closeWith closes the stream, and sends a bye first if sendBye is set.
func (s *Stream) closeWith(err error, sendBye bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if err == nil {
		err = ErrClosed
	}
	s.closed = true
	s.closeErr = err
	close(s.done)
	s.cond.Broadcast()
	a := s.cur
	s.mu.Unlock()
	if a == nil {
		return
	}

	if sendBye {
		// Don't hold mu while writing.
		s.writeMu.Lock()
		WriteJSON(a.t, Message{Type: "bye"})
		s.writeMu.Unlock()
	}
	s.mu.Lock()
	s.stop(a, err)
	s.mu.Unlock()
}

// Done returns a channel that is closed when the stream closes.
func (s *Stream) Done() <-chan struct{} {
	return s.done
}

// Err returns the reason the stream closed, or nil while it is open.
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// Received returns how many data bytes have arrived from the peer. It is the
// number to give the peer when re-attaching.
func (s *Stream) Received() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

// Sent returns how many bytes have been passed to Write.
func (s *Stream) Sent() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent
}

// Detach stops the current transport, if any, and waits for its goroutines to
// exit. Afterwards, Received no longer changes until the next Attach.
func (s *Stream) Detach() {
	s.mu.Lock()
	a := s.cur
	if a != nil {
		s.stop(a, ErrReplaced)
	}
	s.mu.Unlock()
	if a != nil {
		a.wg.Wait()
	}
}

// Attach connects the stream to t. peerReceived is how many bytes the peer
// says it has received; the stream drops those from its buffer and resends the
// rest. A transport that is already attached is stopped first.
//
// The returned channel receives the reason the transport stopped, and is then
// closed, after all the goroutines using t have exited. If the stream is
// closed, or peerReceived is impossible, Attach returns an error and leaves t
// alone: closing it is up to the caller.
func (s *Stream) Attach(t Transport, peerReceived uint64) (<-chan error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed {
			return nil, s.closeErr
		}
		old := s.cur
		if old == nil {
			break
		}
		s.stop(old, ErrReplaced)
		s.mu.Unlock()
		old.wg.Wait()
		s.mu.Lock()
	}

	acked := s.acked()
	if peerReceived < acked || peerReceived > s.sent {
		return nil, fmt.Errorf("resume: the peer has received %d bytes, but %d to %d are possible", peerReceived, acked, s.sent)
	}
	s.unacked = s.unacked[peerReceived-acked:]

	a := &attachment{
		t:          t,
		nextToSend: peerReceived,
		ackDue:     true,
		quit:       make(chan struct{}),
		done:       make(chan error, 1),
	}
	s.cur = a
	a.wg.Add(3)
	go s.readLoop(a)
	go s.sendLoop(a)
	go s.heartbeatLoop(a, s.heartbeat)
	return a.done, nil
}

// stop stops the attachment: it closes the transport and makes its goroutines
// exit. Only the first call counts. Call it with mu held. It never blocks: the
// reason is delivered on a.done from a separate goroutine, once the
// attachment's goroutines have exited.
func (s *Stream) stop(a *attachment, reason error) {
	if a.stopped {
		return
	}
	a.stopped = true
	a.reason = reason
	if s.cur == a {
		s.cur = nil
	}
	close(a.quit)
	s.cond.Broadcast()
	a.t.Close()
	go func() {
		a.wg.Wait()
		a.done <- reason
		close(a.done)
	}()
}

// readLoop reads the transport until it fails or the attachment stops.
func (s *Stream) readLoop(a *attachment) {
	defer a.wg.Done()
	for {
		text, data, err := a.t.ReadMessage()
		if err != nil {
			s.mu.Lock()
			s.stop(a, err)
			s.mu.Unlock()
			return
		}

		s.mu.Lock()
		if a.stopped || s.cur != a {
			// The peer will resend this data after the next Attach.
			s.mu.Unlock()
			return
		}
		if !text {
			s.readBuf = append(s.readBuf, data...)
			s.received += uint64(len(data))
			a.ackDue = true
			s.cond.Broadcast()
			s.mu.Unlock()
			continue
		}

		var m Message
		if err := json.Unmarshal(data, &m); err != nil {
			s.mu.Unlock()
			s.Close(fmt.Errorf("resume: bad message from the peer: %v", err))
			return
		}
		switch m.Type {
		case "ack":
			acked := s.acked()
			if m.Received < acked || m.Received > s.sent {
				s.mu.Unlock()
				s.Close(fmt.Errorf("resume: bad ack: %d bytes, but %d to %d are possible", m.Received, acked, s.sent))
				return
			}
			s.unacked = s.unacked[m.Received-acked:]
			s.cond.Broadcast()
			s.mu.Unlock()
		case "end":
			s.peerEnded = true
			s.cond.Broadcast()
			s.mu.Unlock()
		case "bye":
			s.mu.Unlock()
			s.closeWith(ErrPeerClosed, false)
			return
		default:
			s.mu.Unlock()
			s.Close(fmt.Errorf("resume: bad message from the peer: unknown type %q", m.Type))
			return
		}
	}
}

// sendLoop is the only goroutine that sends data, acks and end on a.
func (s *Stream) sendLoop(a *attachment) {
	defer a.wg.Done()
	for {
		text, data, ok := s.nextMessage(a)
		if !ok {
			return
		}

		s.writeMu.Lock()
		s.mu.Lock()
		dead := a.stopped || s.closed
		s.mu.Unlock()
		if dead {
			s.writeMu.Unlock()
			return
		}
		err := a.t.WriteMessage(text, data)
		s.writeMu.Unlock()
		if err != nil {
			s.mu.Lock()
			s.stop(a, err)
			s.mu.Unlock()
			return
		}
	}
}

// nextMessage waits until there is something to send on a, and returns it.
// It returns false when the attachment is stopped or the stream is closed.
func (s *Stream) nextMessage(a *attachment) (text bool, data []byte, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if a.stopped || s.closed {
			return false, nil, false
		}
		if a.ackDue {
			a.ackDue = false
			return true, mustMarshal(Message{Type: "ack", Received: s.received}), true
		}
		if a.nextToSend < s.acked() {
			// The peer acknowledged more than we sent on this transport.
			a.nextToSend = s.acked()
		}
		if a.nextToSend < s.sent {
			start := a.nextToSend - s.acked()
			end := min(start+maxMessage, uint64(len(s.unacked)))
			// Copy, because unacked can be reallocated once mu is released.
			data = append([]byte(nil), s.unacked[start:end]...)
			a.nextToSend += uint64(len(data))
			return false, data, true
		}
		if s.ended && !a.endSent {
			a.endSent = true
			return true, mustMarshal(Message{Type: "end", Received: s.received}), true
		}
		s.cond.Wait()
	}
}

// heartbeatLoop makes the sender send an ack every interval, even when there is
// nothing new to acknowledge. The traffic keeps the connection alive, and
// shows a dead one as a write error.
func (s *Stream) heartbeatLoop(a *attachment, interval time.Duration) {
	defer a.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-a.quit:
			return
		case <-ticker.C:
			s.mu.Lock()
			a.ackDue = true
			s.cond.Broadcast()
			s.mu.Unlock()
		}
	}
}

func mustMarshal(m Message) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err) // Message always marshals.
	}
	return b
}

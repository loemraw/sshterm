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

package websocket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall/js"
	"time"

	"github.com/c2FmZQ/sshterm/internal/jsutil"
)

var (
	ErrClosed         = errors.New("websocket is closed")
	ErrClosedByServer = errors.New("websocket was closed by server")
)

func New(ctx context.Context, url string, log io.Writer) (*WebSocket, error) {
	var jsWS js.Value
	var err error
	jsutil.TryCatch(
		func() { // try
			jsWS = js.Global().Get("WebSocket").New(url)
		},
		func(e any) { // catch
			err = fmt.Errorf("websocket: %v", e)
		},
	)
	if err != nil {
		return nil, err
	}
	jsWS.Set("binaryType", "arraybuffer")
	ws := &WebSocket{
		ctx:     ctx,
		ws:      jsWS,
		log:     log,
		ch:      make(chan message, 4096),
		closeCh: make(chan struct{}),
	}
	errCh := make(chan error, 1)
	openFn := js.FuncOf(func(this js.Value, args []js.Value) any {
		select {
		case errCh <- nil:
		default:
		}
		return nil
	})
	errorFn := js.FuncOf(func(this js.Value, args []js.Value) any {
		select {
		case errCh <- fmt.Errorf("websocket error"):
		default:
		}
		ws.Close()
		return nil
	})
	closeFn := js.FuncOf(func(this js.Value, args []js.Value) any {
		if !ws.isClosed() {
			ws.err = ErrClosedByServer
		}
		select {
		case errCh <- ErrClosedByServer:
		default:
		}
		ws.Close()
		return nil
	})
	msgFn := js.FuncOf(func(this js.Value, args []js.Value) any {
		data := args[0].Get("data")
		msg := message{data: data}
		switch {
		case data.Type() == js.TypeString:
			msg.text = true
		case data.InstanceOf(jsutil.ArrayBuffer):
		default:
			if !ws.isClosed() {
				ws.err = errors.New("websocket: unexpected message type")
			}
			return ws.Close()
		}
		select {
		case <-ws.ctx.Done():
			return ws.Close()
		case ws.ch <- msg:
		}
		return nil
	})
	ws.cleanup = func() {
		jsWS.Call("removeEventListener", "open", openFn)
		jsWS.Call("removeEventListener", "error", errorFn)
		jsWS.Call("removeEventListener", "close", closeFn)
		jsWS.Call("removeEventListener", "message", msgFn)
		openFn.Release()
		errorFn.Release()
		closeFn.Release()
		msgFn.Release()
	}
	jsWS.Call("addEventListener", "open", openFn)
	jsWS.Call("addEventListener", "error", errorFn)
	jsWS.Call("addEventListener", "close", closeFn)
	jsWS.Call("addEventListener", "message", msgFn)
	select {
	case <-ctx.Done():
		ws.Close()
		return nil, ctx.Err()
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
		return ws, nil
	}
}

var _ net.Conn = (*WebSocket)(nil)

// message is one WebSocket message: a string if text is set, else an
// ArrayBuffer.
type message struct {
	text bool
	data js.Value
}

type WebSocket struct {
	ctx     context.Context
	ws      js.Value // WebSocket
	log     io.Writer
	ch      chan message
	r       []byte
	closeCh chan struct{}
	err     error
	cleanup func()
}

func (ws *WebSocket) isClosed() bool {
	select {
	case <-ws.closeCh:
		return true
	default:
		return false
	}

}

func (ws *WebSocket) Close() error {
	if !ws.isClosed() {
		if ws.err == nil {
			ws.err = ErrClosed
		}
		close(ws.closeCh)
		ws.ws.Call("close")
		if ws.cleanup != nil {
			ws.cleanup()
		}
	}
	return nil
}

func (ws *WebSocket) readChunk() error {
	select {
	case <-ws.ctx.Done():
		return ws.ctx.Err()
	case msg := <-ws.ch:
		if msg.text {
			return errors.New("websocket: unexpected message type")
		}
		vv := jsutil.Uint8Array.New(msg.data)
		n := len(ws.r)
		ws.r = append(ws.r, make([]byte, vv.Length())...)
		js.CopyBytesToGo(ws.r[n:], vv)
		return nil
	case <-ws.closeCh:
		return ws.err
	}
}

func (ws *WebSocket) Read(b []byte) (int, error) {
	var err error
	for len(ws.r) == 0 || (len(ws.r) < len(b) && len(ws.ch) > 0) {
		if err = ws.readChunk(); err != nil {
			break
		}
	}
	n := copy(b, ws.r)
	ws.r = ws.r[n:]
	return n, err
}

func (ws *WebSocket) Write(b []byte) (int, error) {
	if ws.isClosed() {
		return 0, ws.err
	}
	n := len(b)
	for len(b) > 0 {
		sz := min(len(b), 4096)
		ws.ws.Call("send", jsutil.Uint8ArrayFromBytes(b[:sz]))
		b = b[sz:]
	}
	return n, nil
}

func (ws *WebSocket) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

func (ws *WebSocket) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (ws *WebSocket) SetDeadline(t time.Time) error {
	return nil
}

func (ws *WebSocket) SetReadDeadline(t time.Time) error {
	return nil
}

func (ws *WebSocket) SetWriteDeadline(t time.Time) error {
	return nil
}

// ReadMessage returns the next message, and whether it's a text message.
// Messages that arrived before the WebSocket closed come before the close
// error, so a server's last word, such as why it refused, isn't lost.
func (ws *WebSocket) ReadMessage() (text bool, data []byte, err error) {
	select {
	case msg := <-ws.ch:
		return msg.bytes()
	default:
	}
	select {
	case <-ws.ctx.Done():
		return false, nil, ws.ctx.Err()
	case msg := <-ws.ch:
		return msg.bytes()
	case <-ws.closeCh:
		return false, nil, ws.err
	}
}

// bytes returns the message's contents, and whether it's a text message.
func (msg message) bytes() (text bool, data []byte, err error) {
	if msg.text {
		return true, []byte(msg.data.String()), nil
	}
	vv := jsutil.Uint8Array.New(msg.data)
	b := make([]byte, vv.Length())
	js.CopyBytesToGo(b, vv)
	return false, b, nil
}

// WriteMessage sends one message: a string for text, an ArrayBuffer view for binary.
func (ws *WebSocket) WriteMessage(text bool, data []byte) error {
	if ws.isClosed() {
		return ws.err
	}
	var err error
	jsutil.TryCatch(
		func() { // try
			if text {
				ws.ws.Call("send", string(data))
			} else {
				ws.ws.Call("send", jsutil.Uint8ArrayFromBytes(data))
			}
		},
		func(e any) { // catch
			err = fmt.Errorf("websocket: %v", e)
		},
	)
	return err
}

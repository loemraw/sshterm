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

//go:build js && wasm

package tests

import (
	"encoding/base64"
	"slices"
	"strings"
	"sync"
	"syscall/js"
	"testing"
	"time"
)

func TestOSC52(t *testing.T) {
	term := js.Global().Get("sshApp").Get("term")
	clipboard := js.Global().Get("navigator").Get("clipboard")
	if clipboard.IsUndefined() {
		t.Fatal("navigator.clipboard is undefined")
	}

	var mu sync.Mutex
	var written, sent []string
	var reads int

	writeText := js.FuncOf(func(this js.Value, args []js.Value) any {
		mu.Lock()
		defer mu.Unlock()
		written = append(written, args[0].String())
		return js.Global().Get("Promise").Call("resolve")
	})
	readText := js.FuncOf(func(this js.Value, args []js.Value) any {
		mu.Lock()
		defer mu.Unlock()
		reads++
		return js.Global().Get("Promise").Call("resolve", "clipboard contents")
	})
	// Deleting the properties restores the prototype's methods.
	clipboard.Set("writeText", writeText)
	clipboard.Set("readText", readText)
	t.Cleanup(func() {
		clipboard.Delete("writeText")
		clipboard.Delete("readText")
		writeText.Release()
		readText.Release()
	})

	onData := js.FuncOf(func(this js.Value, args []js.Value) any {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, args[0].String())
		return nil
	})
	disp := term.Call("onData", onData)
	t.Cleanup(func() {
		disp.Call("dispose")
		onData.Release()
	})

	write := func(data string) {
		done := make(chan struct{})
		cb := js.FuncOf(func(this js.Value, args []js.Value) any {
			close(done)
			return nil
		})
		defer cb.Release()
		term.Call("write", data, cb)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for term.write")
		}
	}
	osc52 := func(selection, text string) string {
		return "\x1b]52;" + selection + ";" + base64.StdEncoding.EncodeToString([]byte(text)) + "\x07"
	}

	write(osc52("c", "from c"))
	write(osc52("", "from tmux"))
	write(osc52("p", "from p"))
	write("\x1b]52;c;?\x07")

	const emptyAnswer = "\x1b]52;c;\x07"
	want := []string{"from c", "from tmux"}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		ok := slices.Equal(written, want) && slices.Contains(sent, emptyAnswer)
		mu.Unlock()
		if ok || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(written, want) {
		t.Errorf("clipboard writes = %q, want %q", written, want)
	}
	if reads != 0 {
		t.Errorf("clipboard reads = %d, want 0", reads)
	}
	if !slices.Contains(sent, emptyAnswer) {
		t.Errorf("terminal sent %q, want %q among them", sent, emptyAnswer)
	}
	for _, s := range sent {
		if strings.HasPrefix(s, "\x1b]52;") && s != emptyAnswer {
			t.Errorf("unexpected OSC 52 answer %q", s)
		}
	}
}

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
	"syscall/js"
	"testing"
	"time"
)

func TestUnicodeWidths(t *testing.T) {
	term := js.Global().Get("sshApp").Get("term")
	// Reading term.unicode without allowProposedApi throws, which stops the test program.
	if !term.Get("options").Get("allowProposedApi").Bool() {
		t.Fatal("allowProposedApi is not set")
	}
	if v := term.Get("unicode").Get("activeVersion").String(); v != "11" {
		t.Fatalf("unicode.activeVersion = %q, want \"11\"", v)
	}

	write := func(data string) {
		done := make(chan struct{}, 1)
		cb := js.FuncOf(func(this js.Value, args []js.Value) any {
			done <- struct{}{}
			return nil
		})
		defer cb.Release()
		term.Call("write", data, cb)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout writing to the terminal")
		}
	}

	write("\r\n✅")
	if x := term.Get("buffer").Get("active").Get("cursorX").Int(); x != 2 {
		t.Errorf("cursorX after U+2705 = %d, want 2", x)
	}
	term.Call("write", "\r\n")
}

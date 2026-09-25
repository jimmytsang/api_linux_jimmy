package job

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"testing/synctest"
)

type readResult struct {
	data string
	err  error
}

// readOnce runs one Read on its own goroutine and delivers the result.
func readOnce(r io.Reader) <-chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := r.Read(buf)
		ch <- readResult{string(buf[:n]), err}
	}()
	return ch
}

func TestOutputLateReader(t *testing.T) {
	o := newOutput()
	o.Write([]byte("hello "))
	o.Write([]byte("world"))
	o.finish()

	// A reader created after the job finished still gets everything.
	got, err := io.ReadAll(o.newReader())
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("got %q, want %q", got, "hello world")
	}
}

func TestOutputManyReaders(t *testing.T) {
	const readers, lines = 20, 1000
	o := newOutput()

	var want bytes.Buffer
	for i := range lines {
		fmt.Fprintf(&want, "line %d\n", i)
	}

	var wg sync.WaitGroup
	results := make([][]byte, readers)
	startReader := func(i int) {
		wg.Go(func() {
			got, err := io.ReadAll(o.newReader())
			if err != nil {
				t.Errorf("reader %d: %v", i, err)
			}
			results[i] = got
		})
	}

	// Half the readers start before any output and follow it live; the rest
	// join halfway through and must catch up from the first byte.
	for i := range readers / 2 {
		startReader(i)
	}
	for i := range lines {
		if i == lines/2 {
			for j := readers / 2; j < readers; j++ {
				startReader(j)
			}
		}
		fmt.Fprintf(o, "line %d\n", i)
	}
	o.finish()
	wg.Wait()

	for i, got := range results {
		if !bytes.Equal(got, want.Bytes()) {
			t.Errorf("reader %d got %d bytes, want %d identical bytes", i, len(got), want.Len())
		}
	}
}

func TestOutputReadBlocksThenWakes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := newOutput()
		res := readOnce(o.newReader())

		// Wait returns once the reader is durably blocked in cond.Wait.
		synctest.Wait()
		select {
		case r := <-res:
			t.Fatalf("Read returned %q, %v before any output", r.data, r.err)
		default:
		}

		o.Write([]byte("hello"))
		synctest.Wait()
		select {
		case r := <-res:
			if r.err != nil || r.data != "hello" {
				t.Fatalf("Read = %q, %v; want %q, nil", r.data, r.err, "hello")
			}
		default:
			t.Fatal("Read still blocked after Write")
		}
	})
}

func TestOutputFinishWakesWithEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := newOutput()
		res := readOnce(o.newReader())
		synctest.Wait()

		o.finish()
		synctest.Wait()
		select {
		case r := <-res:
			if r.err != io.EOF {
				t.Fatalf("Read err = %v, want io.EOF", r.err)
			}
		default:
			t.Fatal("Read still blocked after finish")
		}
	})
}

func TestOutputCloseUnblocksRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := newOutput()
		closing, other := o.newReader(), o.newReader()
		closingRes, otherRes := readOnce(closing), readOnce(other)
		synctest.Wait()

		closing.Close()
		synctest.Wait()
		select {
		case r := <-closingRes:
			if !errors.Is(r.err, io.ErrClosedPipe) {
				t.Fatalf("Read err = %v, want io.ErrClosedPipe", r.err)
			}
		default:
			t.Fatal("Read still blocked after Close")
		}

		// Closing one reader broadcasts to all of them. The other reader must
		// wake, see nothing it cares about, and go back to waiting.
		select {
		case r := <-otherRes:
			t.Fatalf("other reader returned %q, %v after an unrelated Close", r.data, r.err)
		default:
		}

		// Reads after Close keep failing, and Close is idempotent.
		if _, err := closing.Read(make([]byte, 8)); !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("Read after Close err = %v, want io.ErrClosedPipe", err)
		}
		closing.Close()

		o.finish()
		if r := <-otherRes; r.err != io.EOF {
			t.Errorf("other reader err = %v, want io.EOF", r.err)
		}
	})
}

func TestOutputBinaryRoundTrip(t *testing.T) {
	// Every byte value, including NUL and bytes that aren't valid UTF-8.
	want := make([]byte, 256)
	for i := range want {
		want[i] = byte(i)
	}

	o := newOutput()
	// Uneven write sizes so chunk boundaries don't line up with the reads.
	for rest := want; len(rest) > 0; {
		n := min(13, len(rest))
		o.Write(rest[:n])
		rest = rest[n:]
	}
	o.finish()

	var got []byte
	r := o.newReader()
	buf := make([]byte, 7)
	for {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Errorf("round trip changed the data:\ngot  %x\nwant %x", got, want)
	}
}

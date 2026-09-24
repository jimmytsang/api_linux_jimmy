package job

import (
	"errors"
	"io"
	"sync"
)

// errReaderClosed is returned by Read once the reader has been closed.
var errReaderClosed = errors.New("output reader closed")

// output holds all of a job's output in a buffer that only ever grows.
//
// It is used directly as cmd.Stdout and cmd.Stderr, so the copier goroutine
// os/exec runs is the only writer. Any number of readers stream from it, each
// at its own position.
type output struct {
	mu   sync.Mutex
	cond *sync.Cond // sync.NewCond(&mu); signals data, done and reader closes
	data []byte     // all output so far
	done bool       // no more output will arrive
}

func newOutput() *output {
	o := &output{}
	o.cond = sync.NewCond(&o.mu)
	return o
}

// Write appends p to the buffer and wakes every waiting reader. It never
// blocks on readers, so a slow client can't slow down the job.
func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.data = append(o.data, p...)
	o.cond.Broadcast()
	return len(p), nil
}

// finish marks the output as complete. Readers that have caught up get io.EOF.
func (o *output) finish() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.done = true
	o.cond.Broadcast()
}

// newReader returns a reader that starts at the first byte of the output.
func (o *output) newReader() *reader {
	return &reader{out: o}
}

// reader is one streaming client's view of the buffer. Its fields are guarded
// by out.mu.
type reader struct {
	out    *output
	off    int  // how far this client has read
	closed bool // set by Close when the client goes away
}

// Read blocks until there is output past the reader's position, the output is
// done (io.EOF), or the reader is closed.
func (r *reader) Read(p []byte) (int, error) {
	o := r.out
	o.mu.Lock()
	for r.off == len(o.data) && !o.done && !r.closed {
		o.cond.Wait() // releases o.mu while blocked, re-acquires on wake
	}
	switch {
	case r.closed:
		o.mu.Unlock()
		return 0, errReaderClosed
	case r.off == len(o.data):
		o.mu.Unlock()
		return 0, io.EOF
	}
	// Snapshot the slice header and claim a range, then copy without the lock.
	// data is append-only, so bytes below len(data) never change: append
	// writes only past len, and a reallocation leaves the old array untouched.
	data, off := o.data, r.off
	n := min(len(data)-off, len(p))
	r.off += n
	o.mu.Unlock()

	return copy(p, data[off:off+n]), nil
}

// Close wakes a Read blocked on this reader and makes it return. It is safe
// to call from another goroutine and more than once.
func (r *reader) Close() error {
	r.out.mu.Lock()
	defer r.out.mu.Unlock()
	r.closed = true
	r.out.cond.Broadcast()
	return nil
}

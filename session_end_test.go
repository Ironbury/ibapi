package ibapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sessionWrapper records errors and connection closes.
type sessionWrapper struct {
	Wrapper
	mu     sync.Mutex
	errs   []sessionErr
	closed chan struct{} // one per ConnectionClosed
}

type sessionErr struct{ id, code int64 }

func newSessionWrapper() *sessionWrapper {
	return &sessionWrapper{closed: make(chan struct{}, 8)}
}

func (w *sessionWrapper) Error(reqID int64, _ int64, errCode int64, _ string, _ string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.errs = append(w.errs, sessionErr{reqID, errCode})
}

func (w *sessionWrapper) ConnectionClosed() { w.closed <- struct{}{} }

func (w *sessionWrapper) seen() []sessionErr {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]sessionErr(nil), w.errs...)
}

func (w *sessionWrapper) hasErr(id, code int64) bool {
	for _, e := range w.seen() {
		if e.id == id && e.code == code {
			return true
		}
	}
	return false
}

func (w *sessionWrapper) waitClosed(t *testing.T, why string) {
	t.Helper()
	select {
	case <-w.closed:
	case <-time.After(3 * time.Second):
		t.Fatal(why)
	}
}

// goid is the calling goroutine's id, as stack dumps name it
// ("goroutine 7 [running]:").
func goid() string {
	buf := make([]byte, 64)
	return strings.Fields(string(buf[:runtime.Stack(buf, false)]))[1]
}

// waitBlocked waits until goroutine id is blocked on a channel inside fn
// (a function's qualified name, such as "ibapi.(*EClient).CancelMktData").
func waitBlocked(t *testing.T, id, fn string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			blocked := strings.HasPrefix(g, "goroutine "+id+" [chan send") || strings.HasPrefix(g, "goroutine "+id+" [select")
			if blocked && strings.Contains(g, fn+"(") {
				return
			}
		}
	}
	t.Fatalf("goroutine %s is not blocked on a channel in %s", id, fn)
}

// fakeTWS answers the API handshake as server version 100 (text messages),
// then reads requests. It never closes a connection itself, so a client's
// reader sees nothing end; the test's cleanup closes them.
type fakeTWS struct {
	ln      net.Listener
	accepts atomic.Int32
	reqs    chan string // every request's payload, from every connection

	mu    sync.Mutex
	conns []net.Conn
}

func startFakeTWS(t *testing.T) *fakeTWS {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeTWS{ln: ln, reqs: make(chan string, 64)}
	go func() {
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			s.accepts.Add(1)
			s.mu.Lock()
			s.conns = append(s.conns, cn)
			s.mu.Unlock()
			go s.serve(cn)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, cn := range s.conns {
			cn.Close()
		}
	})
	return s
}

func (s *fakeTWS) addr() (string, int) {
	a := s.ln.Addr().(*net.TCPAddr)
	return a.IP.String(), a.Port
}

func (s *fakeTWS) serve(cn net.Conn) {
	r := bufio.NewReader(cn)
	if _, err := io.ReadFull(r, make([]byte, 4)); err != nil { // "API\x00"
		return
	}
	if _, err := readFrame(r); err != nil { // the client's version range
		return
	}
	if _, err := cn.Write(frame("100\x0020261016 07:00:00 GMT\x00")); err != nil {
		return
	}
	for {
		p, err := readFrame(r)
		if err != nil {
			return
		}
		s.reqs <- string(p)
	}
}

// awaitReq waits for a request whose payload starts with prefix.
func (s *fakeTWS) awaitReq(t *testing.T, prefix string) {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case p := <-s.reqs:
			if strings.HasPrefix(p, prefix) {
				return
			}
		case <-timeout:
			t.Fatalf("no request %q reached TWS", prefix)
		}
	}
}

func frame(payload string) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(payload))), payload...)
}

func readFrame(r io.Reader) ([]byte, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	p := make([]byte, binary.BigEndian.Uint32(n[:]))
	_, err := io.ReadFull(r, p)
	return p, err
}

// A request waiting for room in the request queue is released when the
// session ends, with the NOT_CONNECTED error on its own id, as a request
// made after the end gets; it does not block its caller for ever.
func TestARequestWaitingForRoomInTheQueueIsReleasedWhenTheSessionEnds(t *testing.T) {
	w := newSessionWrapper()
	c := NewEClient(w)
	// a live session whose requester has stalled: nothing reads the queue
	atomic.StoreInt32(&c.conn.isConnected, 1)
	c.setConnState(CONNECTED)
	for range cap(c.reqChan) {
		c.ReqCurrentTime()
	}
	mktData, order := make(chan struct{}), make(chan struct{})
	mktDataID, orderID := make(chan string, 1), make(chan string, 1)
	go func() {
		defer close(mktData)
		mktDataID <- goid()
		c.CancelMktData(77)
	}()
	go func() {
		defer close(order)
		orderID <- goid()
		c.CancelOrder(88, NewOrderCancel())
	}()
	waitBlocked(t, <-mktDataID, "ibapi.(*EClient).CancelMktData")
	waitBlocked(t, <-orderID, "ibapi.(*EClient).CancelOrder")

	if err := c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	for _, done := range []chan struct{}{mktData, order} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("a request waiting for room in the queue was never released")
		}
	}
	c.CancelMktData(99)
	for _, id := range []int64{77, 88, 99} {
		if !w.hasErr(id, NOT_CONNECTED.Code) {
			t.Errorf("no NOT_CONNECTED error for request %d: %v", id, w.seen())
		}
	}
}

// Once the session has ended (a read error cancels it before the library
// marks itself disconnected), a request is refused with NOT_CONNECTED even
// with room in the queue, rather than queued where nothing reads it.
func TestARequestAfterTheSessionEndedIsRefusedEvenWithRoomInTheQueue(t *testing.T) {
	w := newSessionWrapper()
	c := NewEClient(w)
	atomic.StoreInt32(&c.conn.isConnected, 1)
	c.setConnState(CONNECTED)
	c.cancel() // what the reader does at a read error
	for id := range int64(cap(c.reqChan)) {
		c.CancelMktData(id)
	}
	if n := len(c.reqChan); n != 0 {
		t.Fatalf("%d requests queued after the session ended", n)
	}
	for id := range int64(cap(c.reqChan)) {
		if !w.hasErr(id, NOT_CONNECTED.Code) {
			t.Errorf("no NOT_CONNECTED error for request %d: %v", id, w.seen())
		}
	}
}

// When the session ends while the decoder's queue is full, the reader
// stops instead of waiting for ever to hand over its next message (the
// decoder has stopped reading), so a Disconnect waiting for it returns.
func TestTheReaderStopsWhenTheSessionEndsWithTheDecodersQueueFull(t *testing.T) {
	var msgs bytes.Buffer
	for range 3 {
		msgs.Write(frame("49\x001\x001700000000\x00"))
	}
	scanner := bufio.NewScanner(&msgs)
	scanner.Split(scanFields)
	ctx, cancel := context.WithCancel(context.Background())
	msgChan := make(chan []byte, 1) // nothing reads it: the first message fills it
	done, id := make(chan struct{}), make(chan string, 1)
	go func() {
		defer close(done)
		id <- goid()
		readMessages(ctx, cancel, scanner, msgChan)
	}()
	waitBlocked(t, <-id, "ibapi.readMessages") // holding the second message

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the reader still waits to hand over a message after the session ended")
	}
}

// A write error fails the write. It does not redial: a fresh socket has
// not done the TWS handshake, so TWS would drop what was written to it.
func TestAWriteErrorFailsTheWriteWithoutARedial(t *testing.T) {
	s := startFakeTWS(t)
	host, port := s.addr()
	conn := &Connection{wrapper: &Wrapper{}}
	if err := conn.connect(host, port); err != nil {
		t.Fatal(err)
	}
	defer conn.disconnect()
	if err := conn.getConn().CloseWrite(); err != nil { // the socket can no longer send
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("API\x00")); err == nil {
		t.Fatal("a write on a socket that cannot send succeeded")
	}
	time.Sleep(200 * time.Millisecond) // time for any redial to land
	if n := s.accepts.Load(); n != 1 {
		t.Fatalf("%d connections, want 1: the write redialled", n)
	}
}

// A write error ends the session, through the shutdown watcher, without
// a redial: whether it comes from the flush of a small request or from
// the direct write of one larger than the writer's buffer (where the
// requester once called Disconnect on itself and waited for itself).
func TestAWriteErrorEndsTheSessionWithoutARedial(t *testing.T) {
	SetLogLevel(4) // the requester logs the whole failed request
	defer SetLogLevel(logLevel)
	for _, tc := range []struct {
		name string
		send func(c *EClient)
	}{
		{"flushed", func(c *EClient) { c.ReqCurrentTime() }},
		{"larger than the buffer", func(c *EClient) {
			contract := NewContract()
			contract.Symbol, contract.SecType, contract.Exchange, contract.Currency = "AAPL", "STK", "SMART", "USD"
			c.ReqMktData(1, contract, strings.Repeat("1,", 3000), false, false, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startFakeTWS(t)
			host, port := s.addr()
			w := newSessionWrapper()
			c := NewEClient(w)
			if err := c.Connect(host, port, 7); err != nil {
				t.Fatal(err)
			}
			s.awaitReq(t, "71\x00") // startAPI
			if err := c.conn.getConn().CloseWrite(); err != nil {
				t.Fatal(err)
			}
			tc.send(c)
			w.waitClosed(t, "the write error did not end the session")
			if c.IsConnected() {
				t.Fatal("still connected after the write error")
			}
			if n := s.accepts.Load(); n != 1 {
				t.Fatalf("%d connections, want 1: the write redialled", n)
			}
		})
	}
}

// Disconnect leaves the ended session's state in place, so a client
// whose session ended refuses to connect again rather than reusing it.
func TestAClientWhoseSessionEndedDoesNotConnectAgain(t *testing.T) {
	s := startFakeTWS(t)
	host, port := s.addr()
	w := newSessionWrapper()
	c := NewEClient(w)
	if err := c.Connect(host, port, 7); err != nil {
		t.Fatal(err)
	}
	s.awaitReq(t, "71\x00")
	c.ReqCurrentTime()
	s.awaitReq(t, "49\x00")
	if err := c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	w.waitClosed(t, "Disconnect did not report the connection closed")

	if err := c.Connect(host, port, 7); !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("Connect: %v, want ErrSessionEnded", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.ConnectWithContext(ctx, host, port, 7); !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("ConnectWithContext: %v, want ErrSessionEnded", err)
	}
	if n := s.accepts.Load(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
}

// A failed connect's reset ends the abandoned session's context, so
// nothing waiting on it waits for ever.
func TestAResetEndsTheAbandonedSession(t *testing.T) {
	c := NewEClient(newSessionWrapper())
	abandoned := c.Ctx()
	c.reset()
	if abandoned.Err() == nil {
		t.Fatal("the abandoned session's context is still live")
	}
	if c.Ctx().Err() != nil {
		t.Fatal("the fresh session's context is already ended")
	}
}

package proplydtest

import (
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"time"
)

// Mode is how a Proxy treats traffic.
type Mode int

const (
	// Forward passes traffic (subject to latency, bandwidth and drops).
	Forward Mode = iota
	// Refuse closes new connections at once, like a dead host's RST.
	Refuse
	// Blackhole accepts connections but delivers nothing in either
	// direction until the mode changes, like a network partition. Data sent
	// meanwhile is delivered afterwards, as TCP retransmission would.
	Blackhole
)

// Proxy is a TCP proxy that injects network faults between a client and an
// upstream.
type Proxy struct {
	t  TB
	ln net.Listener

	mu        sync.Mutex
	upstream  string
	mode      Mode
	latency   time.Duration
	jitter    time.Duration
	bandwidth int     // bytes per second per direction, 0 = unlimited
	dropRate  float64 // probability per chunk of resetting the connection
	modeCh    chan struct{}
	conns     map[*proxyConn]struct{}
	closed    bool
}

type proxyConn struct {
	client, upstream net.Conn
	once             sync.Once
	done             chan struct{}
}

func (c *proxyConn) reset() {
	c.once.Do(func() {
		close(c.done)
		for _, nc := range []net.Conn{c.client, c.upstream} {
			if tc, ok := nc.(*net.TCPConn); ok {
				_ = tc.SetLinger(0) // RST instead of FIN
			}
			_ = nc.Close()
		}
	})
}

// NewProxy listens on a free local port and forwards to upstream.
func NewProxy(t TB, upstream string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{t: t, ln: ln, upstream: upstream, modeCh: make(chan struct{}), conns: map[*proxyConn]struct{}{}}
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// Addr is the address clients connect to.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// SetUpstream changes where new connections go.
func (p *Proxy) SetUpstream(addr string) {
	p.mu.Lock()
	p.upstream = addr
	p.mu.Unlock()
}

// SetMode changes the fault mode. Refuse also resets open connections.
func (p *Proxy) SetMode(m Mode) {
	p.mu.Lock()
	p.mode = m
	close(p.modeCh)
	p.modeCh = make(chan struct{})
	p.mu.Unlock()
	if m == Refuse {
		p.ResetAll()
	}
}

// SetLatency delays every chunk by latency plus a random jitter in
// [0, jitter), preserving order.
func (p *Proxy) SetLatency(latency, jitter time.Duration) {
	p.mu.Lock()
	p.latency, p.jitter = latency, jitter
	p.mu.Unlock()
}

// SetBandwidth limits throughput per direction (0 = unlimited).
func (p *Proxy) SetBandwidth(bytesPerSecond int) {
	p.mu.Lock()
	p.bandwidth = bytesPerSecond
	p.mu.Unlock()
}

// SetDropRate resets a connection with this probability per forwarded
// chunk, like a flaky link.
func (p *Proxy) SetDropRate(rate float64) {
	p.mu.Lock()
	p.dropRate = rate
	p.mu.Unlock()
}

// Clear removes every fault.
func (p *Proxy) Clear() {
	p.SetLatency(0, 0)
	p.SetBandwidth(0)
	p.SetDropRate(0)
	p.SetMode(Forward)
}

// ResetAll resets every open connection.
func (p *Proxy) ResetAll() {
	p.mu.Lock()
	conns := p.conns
	p.conns = map[*proxyConn]struct{}{}
	p.mu.Unlock()
	for c := range conns {
		c.reset()
	}
}

// Close stops the proxy.
func (p *Proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.modeCh)
	p.modeCh = make(chan struct{})
	p.mu.Unlock()
	_ = p.ln.Close()
	p.ResetAll()
}

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *Proxy) handle(client net.Conn) {
	p.mu.Lock()
	mode, upstream := p.mode, p.upstream
	p.mu.Unlock()
	if mode == Refuse {
		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = client.Close()
		return
	}
	up, err := net.DialTimeout("tcp", upstream, time.Second)
	if err != nil {
		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = client.Close()
		return
	}
	pc := &proxyConn{client: client, upstream: up, done: make(chan struct{})}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		pc.reset()
		return
	}
	p.conns[pc] = struct{}{}
	p.mu.Unlock()
	go p.pipe(pc, client, up)
	go p.pipe(pc, up, client)
}

type chunk struct {
	data []byte
	at   time.Time
}

// pipe copies src to dst through a delay queue.
func (p *Proxy) pipe(pc *proxyConn, src, dst net.Conn) {
	queue := make(chan chunk, 1024)
	go func() {
		defer close(queue)
		var last time.Time
		for {
			buf := make([]byte, 32<<10)
			n, err := src.Read(buf)
			if n > 0 {
				p.mu.Lock()
				lat, jit := p.latency, p.jitter
				p.mu.Unlock()
				at := time.Now().Add(lat)
				if jit > 0 {
					at = at.Add(time.Duration(rand.Int64N(int64(jit))))
				}
				if at.Before(last) {
					at = last // keep order
				}
				last = at
				select {
				case queue <- chunk{data: buf[:n], at: at}:
				case <-pc.done:
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		// Half-close so the other side sees EOF, then let the other pipe
		// finish; a reset tears both down.
		if tc, ok := dst.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		} else {
			pc.reset()
		}
	}()
	for c := range queue {
		if d := time.Until(c.at); d > 0 {
			time.Sleep(d)
		}
		// Hold data while blackholed.
		for {
			p.mu.Lock()
			mode, ch, closed := p.mode, p.modeCh, p.closed
			p.mu.Unlock()
			if closed {
				pc.reset()
				return
			}
			if mode != Blackhole {
				break
			}
			select {
			case <-ch:
			case <-pc.done:
				return
			}
		}
		p.mu.Lock()
		bw, drop := p.bandwidth, p.dropRate
		p.mu.Unlock()
		if drop > 0 && rand.Float64() < drop {
			pc.reset()
			return
		}
		if _, err := dst.Write(c.data); err != nil {
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
				pc.reset()
			}
			return
		}
		if bw > 0 {
			time.Sleep(time.Duration(float64(len(c.data)) / float64(bw) * float64(time.Second)))
		}
	}
}

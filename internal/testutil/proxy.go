// Package testutil contains helpers for integration, chaos and stress tests.
package testutil

import (
	"io"
	"net"
	"sync"
)

// ProxyMode controls how the proxy treats traffic.
type ProxyMode int

const (
	// Forward traffic normally.
	Forward ProxyMode = iota
	// Refuse: drop existing connections and reset new ones immediately
	// (the peer sees "connection refused/reset", like a crashed process).
	Refuse
	// Blackhole: keep connections open but never forward a byte (the peer
	// sees timeouts, like a network partition).
	Blackhole
)

// Proxy is a TCP proxy used to simulate network failures between clients
// and the cache. The upstream can be changed at runtime, which lets tests
// restart the cache on a new port behind a stable address.
type Proxy struct {
	lis net.Listener

	mu       sync.Mutex
	upstream string
	mode     ProxyMode
	conns    map[net.Conn]struct{}
	closed   bool
}

// NewProxy listens on a random local port.
func NewProxy(upstream string) (*Proxy, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{lis: lis, upstream: upstream, conns: map[net.Conn]struct{}{}}
	go p.serve()
	return p, nil
}

// Addr is the address clients should dial.
func (p *Proxy) Addr() string { return p.lis.Addr().String() }

// SetUpstream changes where new connections are forwarded.
func (p *Proxy) SetUpstream(addr string) {
	p.mu.Lock()
	p.upstream = addr
	p.mu.Unlock()
}

// SetMode changes the failure mode. Switching away from Forward drops every
// existing connection.
func (p *Proxy) SetMode(m ProxyMode) {
	p.mu.Lock()
	p.mode = m
	var drop []net.Conn
	if m != Forward {
		for c := range p.conns {
			drop = append(drop, c)
		}
		clear(p.conns)
	}
	p.mu.Unlock()
	for _, c := range drop {
		_ = c.Close()
	}
}

// DropConnections closes all current connections without changing the mode.
func (p *Proxy) DropConnections() {
	p.mu.Lock()
	drop := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		drop = append(drop, c)
	}
	clear(p.conns)
	p.mu.Unlock()
	for _, c := range drop {
		_ = c.Close()
	}
}

// Close stops the proxy.
func (p *Proxy) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.SetMode(Refuse)
	return p.lis.Close()
}

func (p *Proxy) track(cs ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	for _, c := range cs {
		p.conns[c] = struct{}{}
	}
	return true
}

func (p *Proxy) serve() {
	for {
		down, err := p.lis.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		mode, upstream := p.mode, p.upstream
		p.mu.Unlock()

		switch mode {
		case Refuse:
			if tc, ok := down.(*net.TCPConn); ok {
				_ = tc.SetLinger(0) // RST instead of FIN
			}
			_ = down.Close()
			continue
		case Blackhole:
			if !p.track(down) {
				_ = down.Close()
			}
			go func() { _, _ = io.Copy(io.Discard, down) }()
			continue
		}

		up, err := net.Dial("tcp", upstream)
		if err != nil {
			_ = down.Close()
			continue
		}
		if !p.track(down, up) {
			_ = down.Close()
			_ = up.Close()
			continue
		}
		go p.pipe(down, up)
		go p.pipe(up, down)
	}
}

func (p *Proxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}

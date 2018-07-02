package p2p

import (
	"net"
	"time"
	"sync"
)

var pingInterval = 15 * time.Second

// mean a transport, can write/read/close
type Transport interface {
	ReadWriter
	Close() error
}

type Conn struct {
	netconn net.Conn
	Transport
}

type Peer struct {
	conn *Conn
	disconnect chan DisconnectReason
	created time.Time
	closed chan struct{}
	protocolErr chan error
	waitDown sync.WaitGroup
}

func NewPeer(c *Conn) *Peer {
	pipe, _ := net.Pipe()
	conn := Conn{
		netconn: pipe,
		Transport: nil,
	}
	return &Peer{
		conn: &conn,
		disconnect: make(chan DisconnectReason),
		created: time.Now(),
		closed: make(chan struct{}),
	}
}

func (p *Peer) Run(svr *Server) {
	p.waitDown.Add(2)

	var canWrite = make(chan struct{})
	var wError = make(chan error)
	var rError = make(chan error)
	var reson DisconnectReason

	go p.readLoop(rError)
	go p.pingLoop()

	p.waitDown.Wait()
	svr.delPeer <- p
}

func (p *Peer) ID() NodeID {

}

func (p *Peer) readLoop(cerr chan<- error) {
	defer p.waitDown.Done()

	for {
		msg, err := p.conn.Read()
		if err != nil {
			cerr <- err
			return
		}
		err = p.handleMsg(msg)
		if err != nil {
			cerr <- err
			return
		}
	}
}

func (p *Peer) pingLoop() {
	defer p.waitDown.Done()

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <- ticker.C:
			if err := Send(p.conn, pingMsg); err != nil {
				p.protocolErr <- err
				return
			}
		case <- p.closed:
			return
		}
	}
}

func (p *Peer) handleMsg(msg Message) error {
	switch msg.code {

	}
	return nil
}

func (p *Peer) Disconnect(reason DisconnectReason) {
	select {
	case p.disconnect <- reason:
	case <- p.closed:
	}
}

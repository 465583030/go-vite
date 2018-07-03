package p2p

import (
	"net"
	"time"
	"sync"
	"github.com/vitelabs/go-vite/p2p/msgs"
)

var pingInterval = 15 * time.Second

// mean a transport, can write/read/close
type Transport interface {
	MsgReadWriter
	Close() error
}

type Conn struct {
	flag ConnFlag
	netconn net.Conn
	Transport
}

func (c *Conn) is(flag ConnFlag) bool {
	return c.flag.is(flag)
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

	loop:
	for {
		select {
			case <- canWrite:

		}
	}

	close(p.closed)
	p.conn.Close()
	p.waitDown.Wait()
	svr.delPeer <- p
}

func (p *Peer) ID() NodeID {

}

func (p *Peer) readLoop(cherr chan<- error) {
	defer p.waitDown.Done()

	for {
		msg, err := p.conn.ReadMsg()
		if err != nil {
			cherr <- err
			return
		}
		err = p.handleMsg(msg)
		if err != nil {
			cherr <- err
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
			if err := Send(p.conn, 1, &msgs.Ping{}); err != nil {
				p.protocolErr <- err
				return
			}
		case <- p.closed:
			return
		}
	}
}

func (p *Peer) handleMsg(msg msgs.Msg) error {
	switch msg.Header.Command {
	case pingMsg:
		Send(p.conn, 2, &msgs.Pong{})
	case pongMsg:
// todo handle other messages
	}
	return nil
}

func (p *Peer) Disconnect(reason DisconnectReason) {
	select {
	case p.disconnect <- reason:
	case <- p.closed:
	}
}

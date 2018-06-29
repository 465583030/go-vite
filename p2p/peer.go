package p2p

import "net"

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
}

func NewPeer(c *Conn) *Peer {
	// todo create new peer
	return nil
}

func (p *Peer) Run(svr *Server) {
	// todo peer task logic

	svr.delPeer <- p
}

func (p *Peer) ID() NodeID {

}

func (p *Peer) Disconnect() {

}

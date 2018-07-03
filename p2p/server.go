// Package p2p implements the vite P2P network

package p2p

import (
	"time"
	"golang.org/x/crypto/ed25519"
	"sync"
	"net"
	"errors"
	"log"
)

const (
	defaultDialTimeout = 10 * time.Second
	defaultMaxPendingPeers uint32 = 30
	defaultMaxActiveDail uint32 = 16
)

var errSvrHasStopped = errors.New("Server has stopped.")


type Config struct {
	// mandatory, `PrivateKey` must be set
	PrivateKey *ed25519.PrivateKey

	// `MaxPeers` is the maximum number of peers that can be connected.
	MaxPeers uint32

	// `MaxPassivePeersRatio` is the ratio of MaxPeers that initiate an active connection to this node.
	// the actual value is `MaxPeers / MaxPassivePeersRatio`
	MaxPassivePeersRatio uint32

	// `MaxPendingPeers` is the maximum number of peers that wait to connect.
	MaxPendingPeers uint32

	BootNodes []*Node

	listenAddr string

	// filepath of database, store former nodes
	Database string
}

type Server struct {
	Config

	running bool

	lock sync.Mutex

	// Wait for shutdown clean jobs done.
	waitDown sync.WaitGroup

	Dialer *NodeDailer

	// TCP listener
	listener *net.TCPListener

	createTransport func(conn net.Conn) Transport

	//nodeTable *Table

	// Indicate whether the server has stopped. If stopped, zero-value can be read from this channel.
	stopped chan struct{}

	addPeer chan *Conn

	delPeer chan *Peer

	// execute operations to peers by sequences.
	peersOps chan peersOperator
	// wait for operation done.
	peersOpsDone chan struct{}
}

type peersOperator func(nodeTable map[NodeID]*Peer)

func (svr *Server) Peers() (peers []*Peer) {
	map2slice := func(nodeTable map[NodeID]*Peer) {
		for _, node := range nodeTable {
			peers = append(peers, node)
		}
	}
	select {
	case svr.peersOps <- map2slice:
		<- svr.peersOpsDone
	case svr.stopped:
	}
	return
}

func (svr *Server) PeersCount() (amount int) {
	count := func(nodeTable map[NodeID]*Peer) {
		amount = len(nodeTable)
	}
	select {
	case svr.peersOps <- count:
		<- svr.peersOpsDone
	case <- svr.stopped:
	}
	return
}

func (svr *Server) MaxActivePeers() uint32 {
	return svr.MaxPeers - svr.MaxPassivePeers()
}

func (svr *Server) MaxPassivePeers() uint32 {
	if svr.MaxPassivePeersRatio == 0 {
		svr.MaxPassivePeersRatio = 3
	}
	return svr.MaxPeers / svr.MaxPassivePeersRatio
}

func (svr *Server) Start() error {
	svr.lock.Lock()
	defer svr.lock.Unlock()

	if svr.running {
		return errors.New("Server is already running.")
	}
	svr.running = true

	if svr.PrivateKey == nil {
		return errors.New("Server.PrivateKey must set, but get nil.")
	}

	if svr.Dialer == nil {
		svr.Dialer = &NodeDailer{
			&net.Dialer{
				Timeout: defaultDialTimeout,
			},
		}
	}

	svr.stopped = make(chan struct{})
	svr.addPeer = make(chan *Conn)
	svr.delPeer = make(chan *Peer)
	svr.peersOps = make(chan peersOperator)
	svr.peersOpsDone = make(chan struct{})

	go svr.Discovery()
	// accept connection
	go svr.Listen()

	go svr.ManageTask()

	return nil
}

func (svr *Server) Discovery() error {
	addr, err := net.ResolveUDPAddr("udp", svr.listenAddr)
	if err != nil {
		return nil
	}
	_, err = net.ListenUDP("udp", addr)
	if err != nil {
		return nil
	}

	// TODO get discovery nodeTable
	//svr.nodeTable =

	return nil
}

func (svr *Server) Listen() error {
	addr, err := net.ResolveTCPAddr("tcp", svr.listenAddr)
	if err != nil {
		return err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	if addr.IP.IsLoopback() {
	//	TODO NAT
	}

	svr.listener = listener
	go svr.handleConn()
	return nil
}

func (svr *Server) handleConn() {
	maxPendingPeers := defaultMaxPendingPeers
	if svr.MaxPendingPeers > 0 {
		maxPendingPeers = svr.MaxPendingPeers
	}

	pending := make(chan struct{}, maxPendingPeers)

	for {
		var conn net.Conn
		var err error

		select {
		case pending <- struct{}{}:
			for {
				conn, err = svr.listener.Accept()
				if err != nil {
					log.Fatal(err)
				} else {
					break
				}
			}

			go svr.SetupConn(conn, pending)
		case <- svr.stopped:
			close(pending)
		}
	}
}

func (svr *Server) SetupActiveConn(conn net.Conn, flag ConnFlag, dest *Node) error {

}
func (svr *Server) SetupPassiveConn(conn net.Conn, flag ConnFlag) error {

}

func (svr *Server) SetupConn(conn net.Conn, pending chan struct{}) {
	c := &Conn{
		netconn: conn,
		Transport: svr.createTransport(conn),
	}
	// todo check & verify
	svr.addPeer <- c
	<- pending
}

func (svr *Server) CheckConn(peers map[NodeID]*Peer, passivePeersCount uint32, conn *Conn) error {
	if uint32(len(peers)) >= svr.MaxPeers {
		return errors.New("to many peers.")
	}
	if passivePeersCount >= svr.MaxPassivePeers() {
		return errors.New("to many passive peers.")
	}
	return nil
}

func (svr *Server) ManageTask() {
	var dialer DialManager
	var peers = make(map[NodeID]*Peer)
	var taskHasDone = make(chan Task, defaultMaxActiveDail)
	var passivePeersCount uint32 = 0
	var activeTasks []Task
	var taskQueue []Task
	delTask := func(d Task) {
		for i, t := range activeTasks {
			if t == d {
				activeTasks = append(activeTasks[:i], activeTasks[i+1:]...)
			}
		}
	}
	runTasks := func(ts []Task) []Task {
		i := 0
		for ; uint32(len(activeTasks)) < defaultMaxActiveDail && i < len(ts); i++ {
			t := ts[i]
			go func() {
				t.Perform(svr)
				taskHasDone <- t
			}()
			activeTasks = append(activeTasks, t)
		}
		return ts[i:]
	}
	scheduleTasks := func() {
		taskQueue = runTasks(taskQueue)
		if uint32(len(activeTasks)) < defaultMaxActiveDail {
			newTasks := dialer.CreateTasks(peers)
			taskQueue = append(taskQueue, newTasks...)
		}
	}

	schedule:
	for {
		scheduleTasks()
		select {
		case <- svr.stopped:
			break schedule
		case t := <- taskHasDone:
			delTask(t)
		case c := <- svr.addPeer:
			err := svr.CheckConn(peers, passivePeersCount, c)
			if err == nil {
				p := NewPeer(c)
				peers[p.ID()] = p
				go p.Run(svr)
				passivePeersCount++
			}
		case p := <- svr.delPeer:
			delete(peers, p.ID())
			passivePeersCount--
		case fn := <- svr.peersOps:
			fn(peers)
			svr.peersOpsDone <- struct{}{}
		}
	}

	for _, p := range peers {
		p.Disconnect(DiscQuitting)
	}

	// wait for peers work down.
	for p := range svr.delPeer {
		delete(peers, p.ID())
	}
}

func (svr *Server) Stop() {
	svr.lock.Lock()
	defer svr.lock.Unlock()

	if !svr.running {
		return
	}

	svr.running = false

	if svr.listener != nil {
		svr.listener.Close()
	}

	close(svr.stopped)
	svr.waitDown.Wait()
}

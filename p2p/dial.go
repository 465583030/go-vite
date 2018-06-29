package p2p

type Task interface {
	Perform(svr *Server)
}

type Dialer interface {
	CreateTasks(peers map[NodeID]*Peer) []Task
	DoneTask(t Task)
}

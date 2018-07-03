package p2p

import (
	"net"
	"time"
	"container/heap"
	"log"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

type ConnFlag int
func (c ConnFlag) is(f ConnFlag) bool {
	return (c & f) != 0
}
const (
	connPassive ConnFlag = 1 << iota
	connActive
)

type Task interface {
	Perform(svr *Server)
}

type discoverTask struct {
	results []*Node
}
func (t *discoverTask) Perform(svr *Server) {

}

type dialTask struct {
	flag ConnFlag
	target *Node
	lastResolved time.Time
	duration time.Duration
}
func (t *dialTask) Perform(svr *Server) {
	conn, err := svr.Dialer.DailNode(t.target)
	if err != nil {
		log.Fatal(err)
	}
	err = svr.SetupActiveConn(conn, t.flag, t.target)
	if err != nil {
		log.Fatal(err)
	}
}

type waitTask struct {
	Duration time.Duration
}
func (t *waitTask) Perform(svr *Server) {
	time.Sleep(t.Duration)
}


type NodeDailer struct {
	*net.Dialer
}

func (d *NodeDailer) DailNode(target *Node) (net.Conn, error) {
	addr := net.TCPAddr{
		IP: target.IP,
		Port: target.Port,
	}
	return d.Dialer.Dial("tcp", addr.String())
}


type DialManager struct {
	maxDials int
	nodeTable Table
	dialing map[NodeID]ConnFlag
	history *dialHistory
	start time.Time
	bootNodes []*Node
	looking bool
	lookResults []*Node
}

func (dm *DialManager) CreateTasks(peers map[NodeID]*Peer) []Task {
	if dm.start.IsZero() {
		dm.start = time.Now()
	}
	dm.history.clean(dm.start)
	var tasks []Task
	addDailTask := func(flag ConnFlag, n *Node) bool {
		if err := dm.checkDial(n, peers); err != nil {
			return false
		}
		dm.dialing[n.ID] = flag
		tasks = append(tasks, &dialTask{
			flag: flag,
			target: n,
		})
		return true
	}

	dials := dm.maxDials
	for _, p := range peers {
		if p.conn.is(connActive) {
			dials--
		}
	}
	for _, f := range dm.dialing {
		if f.is(connActive) {
			dials--
		}
	}

	if len(peers) == 0 && len(dm.bootNodes) > 0 && dials > 0 {
		bootNode := dm.bootNodes[0]
		dm.bootNodes = append(dm.bootNodes[1:], bootNode)
		if addDailTask(connActive, bootNode) {
			dials--
		}
	}

	resultIndex := 0
	for ; resultIndex < len(dm.lookResults); resultIndex++ {
		if addDailTask(connActive, dm.lookResults[resultIndex]) {
			dials--
		}
	}
	dm.lookResults = dm.lookResults[resultIndex:]

	return tasks
}
func (dm *DialManager) TaskDone(t Task, time time.Time) {
	switch t2 := t.(type) {
	case *dialTask:
		dm.history.add(t2.target.ID, time)
		delete(dm.dialing, t2.target.ID)
	case *discoverTask:
		dm.looking = false
		dm.lookResults = append(dm.lookResults, t2.results...)
	}
}

func (dm *DialManager) checkDial(n *Node, peers map[NodeID]*Peer) error {
	_, exist := dm.dialing[n.ID];
	if exist {
		return errors.New("is dialing.")
	}
	if peers[n.ID] != nil {
		return errors.New("has connected.")
	}

	return nil
}

func NewDialManager(maxDials int, tab Table, bootNodes []*Node) *DialManager {
	return &DialManager{
		maxDials: maxDials,
		nodeTable: tab,
		bootNodes: bootNodes,
		history: new(dialHistory),
		dialing: make(map[NodeID]ConnFlag),
	}
}

type dialRecord struct {
	target NodeID
	time time.Time
}
type dialHistory []dialRecord

func (h *dialHistory) earliest() dialRecord {
	return (*h)[0]
}
func (h *dialHistory) add(target NodeID, time time.Time)  {
	heap.Push(h, dialRecord{target, time})
}
func (h *dialHistory) remove(target NodeID) bool {
	index := h.find(target)
	if index >= 0 {
		heap.Remove(h, index)
		return true
	}
	return false
}
func (h *dialHistory) contains(target NodeID) bool {
	index := h.find(target)
	return index >= 0
}
func (h *dialHistory) find(target NodeID) int {
	for i, r := range *h {
		if r.target == target {
			return i
		}
	}
	return -1
}
// clean the former records.
func (h *dialHistory) clean(t time.Time) {
	for h.Len() > 0 && h.earliest().time.Before(t) {
		heap.Pop(h)
	}
}
func (h *dialHistory) Len() int {
	return len(*h)
}
func (h *dialHistory) Less(i, j int) bool {
	return (*h)[i].time.Before((*h)[j].time)
}
func (h *dialHistory) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}
func (h *dialHistory) Pop() interface{} {
	lastIndex := len(*h) - 1
	r := (*h)[lastIndex]
	*h = (*h)[:lastIndex]
	return r
}
func (h *dialHistory) Push(r interface{}) {
	*h = append(*h, r.(dialRecord))
}

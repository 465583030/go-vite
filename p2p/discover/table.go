package discover

import (
	"sort"
	"net"
	"math/rand"
	"sync"
	"encoding/binary"
	"time"
	"github.com/vitelabs/go-vite/crypto"
	crand "crypto/rand"
)

// KAD algorithm
const K = 16
const Candidates = 10
type Bucket struct {
	nodes []*Node
	candidates []*Node
}
const N = hashBits / 15
const bucketMinDistance = hashBits - N
const alpha = 3

func NewBucket() *Bucket {
	return &Bucket{
		nodes: make([]*Node, 0, K),
		candidates: make([]*Node, 0, Candidates),
	}
}

// if n exists in b.nodes, move n to head, return true.
// if b.nodes is not full, set n to the first item, return true.
// if consider n as a candidate, then unshift n to b.candidates.
// return false.
func (b *Bucket) Check(n *Node, asCandidate bool) bool {
	var i = 0
	for i, node := range b.nodes {
		if node == nil {
			break
		}
		// if n exists in b, then move n to head.
		if node.ID == n.ID {
			for j := i; j > 0; j-- {
				b.nodes[j] = b.nodes[j-1]
			}
			b.nodes[0] = n
			return true
		}
	}

	// if nodes is not full, set n to the first item
	if i < cap(b.nodes) {
		n.addTime = time.Now()
		unshiftNode(b.nodes, n)
		b.candidates = removeNode(b.candidates, n)
		return true
	}

	if asCandidate {
		unshiftNode(b.candidates, n)
	}

	return false
}

// obsolete the last node in b.nodes
func (b *Bucket) Obsolete(last *Node) *Node {
	if len(b.nodes) == 0 || b.nodes[len(b.nodes) - 1].ID != last.ID {
		return nil
	}
	if len(b.candidates) == 0 {
		b.nodes = removeNode(b.nodes, last)
		return nil
	}

	candidate := b.candidates[0]
	copy(b.candidates, b.candidates[1:])
	b.nodes[len(b.nodes) - 1] = candidate
	return candidate
}

func (b *Bucket) Remove(n *Node) {
	b.nodes = removeNode(b.nodes, n)
}

func removeNode(nodes []*Node, node *Node) []*Node {
	for i, n := range nodes {
		if n.ID == node.ID {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nodes
}

// put node at first place of nodes without increase capacity, return the obsolete node.
func unshiftNode(nodes []*Node, node *Node) *Node {
	// if node exist in nodes, then move to first.
	var i = 0
	for i, n := range nodes {
		if n.ID == node.ID {
			nodes[0], nodes[i] = nodes[i], nodes[0]
			return nil
		}
	}

	if i < cap(nodes) {
		copy(nodes[1:], nodes)
		nodes[0] = node
		return nil
	}

	// nodes is full, obsolete the last one.
	obs := nodes[i - 1]
	for i := i - 1; i > 0; i-- {
		nodes[i] = nodes[i - 1]
	}
	nodes[0] = node
	return obs
}

// unshift node to nodes, without increase capacity.
//func safeUnshift(nodes []*Node, node *Node) *Node {
//	length := len(nodes)
//	obs := nodes[length - 1]
//	for i := length - 1; i > 0; i-- {
//		nodes[i] = nodes[i - 1]
//	}
//	nodes[0] = node
//	return obs
//}

// discover other nodes through implementations of transport
type transport interface {
	ping(id NodeID, addr *net.UDPAddr) error
	findNode(id NodeID, addr *net.UDPAddr, target NodeID) ([]*Node, error)
	close()
}

const seedNodesCount = 30
const maxFindnodeFailures = 5
var seedNodesMaxAge = 5 * 24 * time.Hour
var refreshInterval = 20 * time.Minute
var revalidateInterval = 10 * time.Second
var minStableDuration = 5 * time.Minute
var storeNodeInterval = 30 * time.Second

// the neighbors table
type Table struct {
	buckets [N]*Bucket
	bootNodes []*Node
	self *Node
	db *NodeDB
	net transport
	rand *rand.Rand
	mutex sync.Mutex
	stopped chan struct{}
	stopReq chan struct{}
	refreshReq chan chan struct{}
}

func NewTable(n *Node, net transport, dbPath string, bootNodes []*Node) (*Table, error) {
	nodeDB, err := newDB(dbPath, dbCurrentVersion, n.ID)
	if err != nil {
		return nil, err
	}

	cps, err := copyNodes(bootNodes)
	if err != nil {
		return nil, err
	}

	tb := &Table{
		self: n,
		db: nodeDB,
		net: net,
		bootNodes: cps,
		rand: rand.New(rand.NewSource(0)),
		stopped: make(chan struct{}),
		stopReq: make(chan struct{}),
		refreshReq: make(chan chan struct{}),
	}

	// init buckets
	for i, _ := range tb.buckets {
		tb.buckets[i] = NewBucket()
	}

	tb.resetRand()

	tb.loadSeedNodes()

	tb.db.initClean()



	return tb, nil
}

func (t *Table) Self() *Node {
	return t.self
}

func (t *Table) resetRand() {
	var b [8]byte
	rand.Read(b[:])

	t.mutex.Lock()
	t.rand.Seed(int64(binary.BigEndian.Uint64(b[:])))
	t.mutex.Unlock()
}

func (t *Table) loadSeedNodes() {
	nodes := t.db.randomNodes(seedNodesCount, seedNodesMaxAge)
	nodes = append(nodes, t.bootNodes...)
	t.AddNodes(nodes)
}

func (t *Table) loop() {
	refreshTicker := time.NewTicker(refreshInterval)
	revalidateTimer := time.NewTimer(t.nextRevalidateDuration())
	storeNodeTimer := time.NewTicker(storeNodeInterval)

	var refreshWaiting []chan struct{}
	refreshDone := make(chan struct{})

	defer refreshTicker.Stop()
	defer revalidateTimer.Stop()
	defer storeNodeTimer.Stop()

	revalidateDone := make(chan struct{})

	go t.refresh(refreshDone)

	loop:
	for {
		select {
		case <- refreshTicker.C:
			t.resetRand()
			if refreshDone == nil {
				 refreshDone = make(chan struct{})
				 go t.refresh(refreshDone)
			}
		case <- revalidateTimer.C:
			go t.revalidate(revalidateDone)
		case <- revalidateDone:
			revalidateTimer.Reset(t.nextRevalidateDuration())
		case req := <- t.refreshReq:
			refreshWaiting = append(refreshWaiting, req)
			if refreshDone == nil {
				refreshDone = make(chan struct{})
				go t.refresh(refreshDone)
			}
		case <- refreshDone:
			for _, w := range refreshWaiting {
				close(w)
			}
			refreshDone = nil
			refreshWaiting = nil
		case <- storeNodeTimer.C:
			go t.storeNodes()
		case <- t.stopReq:
			break loop
		}
	}

	// stop
	if t.net != nil {
		t.net.close()
	}
	if refreshDone != nil {
		<- refreshDone
	}
	for _, w := range refreshWaiting {
		close(w)
	}
	if t.db != nil {
		t.db.close()
	}
	close(t.stopped)
}

func (t *Table) preRefresh() <-chan struct{} {
	done := make(chan struct{})
	select {
	case t.refreshReq <- done:
	case <-t.stopped:
		close(done)
	}
	return done
}

func (t *Table) refresh(refreshDone chan struct{}) {
	defer close(refreshDone)
	t.loadSeedNodes()
	t.Lookup(t.self.ID, false)
	for i := 0; i < alpha; i++ {
		var target NodeID
		crand.Read(target[:])
		t.Lookup(target, false)
	}
}

func (t *Table) nextRevalidateDuration() time.Duration {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return time.Duration(t.rand.Int63n(int64(revalidateInterval)))
}

func (t *Table) nodeNeedRevalidate() (nodes *Node, bucketIndex int) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for _, bucketIndex = range t.rand.Perm(len(t.buckets)) {
		bucket := t.buckets[bucketIndex]
		if len(bucket.nodes) > 0 {
			return bucket.nodes[len(bucket.nodes) - 1], bucketIndex
		}
	}

	return nil, 0
}

func (t *Table) revalidate(done chan<- struct{}) {
	defer func() {
		done <- struct{}{}
	}()

	node, bucketIndex := t.nodeNeedRevalidate()
	if node == nil {
		return
	}

	err := t.net.ping(node.ID, node.addr())

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if err != nil {
		t.buckets[bucketIndex].Obsolete(node)
	} else {
		t.buckets[bucketIndex].Check(node, true)
	}
}

func (t *Table) AddNode(n *Node) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.self.ID == n.ID {
		return
	}
	bucket := t.getBucket(n.hash)

	bucket.Check(n, true)
}

func (t *Table) AddNodes(nodes []*Node) {
	for _, node := range nodes {
		t.AddNode(node)
	}
}

func (t *Table) Lookup(id NodeID, refreshIfEmpty bool) []*Node {
	var hash Hash
	copy(hash[:], crypto.Hash(32, id[:]))
	asked := make(map[NodeID]bool)
	seen := make(map[NodeID]bool)
	reply := make(chan []*Node, alpha)
	pendingQueries := 0
	var result *Closest

	asked[t.self.ID] = true

	for {
		t.mutex.Lock()
		result = t.ClosestNodes(hash, K)
		t.mutex.Unlock()
		if len(result.Nodes) > 0 || !refreshIfEmpty {
			break
		}

		<-t.preRefresh()
		refreshIfEmpty = false
	}

	for {
		for i := 0; i < len(result.Nodes) && pendingQueries < alpha; i++ {
			n := result.Nodes[i]
			if !asked[n.ID] {
				asked[n.ID] = true
				pendingQueries++
				go t.findNode(n, id, reply)
			}
		}

		if pendingQueries == 0 {
			break
		}

		for nodes := range reply {
			pendingQueries--
			for _, node := range nodes {
				if node != nil && !seen[node.ID] {
					seen[node.ID] = true
					result.Push(node, K)
				}
			}
		}
	}

	return result.Nodes
}

func (t *Table) findNode(n *Node, target NodeID, reply chan<- []*Node) {
	results, err := t.net.findNode(n.ID, n.addr(), target)
	fails := t.db.findFails(n.ID)
	if err != nil || len(results) == 0 {
		fails++
		t.db.updateFindFails(n.ID, fails)
		if fails > maxFindnodeFailures {
			t.RemoveNode(n)
		}
	} else if fails > 0 {
		fails--
		t.db.updateFindFails(n.ID, fails)
	}

	for _, n := range results {
		t.AddNode(n)
	}

	reply <- results
}

func (t *Table) RemoveNode(n *Node) {
	if n.ID == t.self.ID {
		return
	}

	bucket := t.buckets[t.self.Distance(n)]
	bucket.Remove(n)
}

func (t *Table) ClosestNodes(target Hash, count int) *Closest {
	closet := &Closest{
		target: target,
	}

	for _, bucket := range t.buckets {
		for _, node := range bucket.nodes {
			closet.Push(node, count)
		}
	}

	return closet
}

func (t *Table) getBucket(target Hash) *Bucket {
	dist := calcDistance(t.self.hash, target)
	if dist < bucketMinDistance {
		return t.buckets[0]
	}
	return t.buckets[dist - bucketMinDistance - 1]
}

func (t *Table) storeNodes() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	now := time.Now()
	for _, b := range t.buckets {
		for _, n := range b.nodes {
			if now.Sub(n.addTime) > minStableDuration {
				t.db.updateNode(n)
			}
		}
	}
}

// Nodes closest to the target NodeID
type Closest struct {
	Nodes []*Node
	target Hash
}

func (c *Closest) Push(n *Node, count int)  {
	length := len(c.Nodes)
	furtherNodeIndex := sort.Search(length, func(i int) bool {
		return disCmp(c.target, c.Nodes[i].hash, n.hash) > 0
	})

	// closest Nodes list is full.
	if length >= count {
		// replace the further one.
		if furtherNodeIndex < length {
			c.Nodes[furtherNodeIndex] = n
		}
	} else {
		// insert n to furtherNodeIndex
		copy(c.Nodes[furtherNodeIndex + 1:], c.Nodes[furtherNodeIndex:])
		c.Nodes[furtherNodeIndex] = n
	}
}

func copyNodes(nodes []*Node) ([]*Node, error) {
	for _, node := range nodes {
		if err := node.Validate(); err != nil {
			return nil, err
		}
	}

	cps := make([]*Node, 0, len(nodes))
	for i, node := range nodes {
		cps[i] = &(*node)
	}

	return cps, nil
}
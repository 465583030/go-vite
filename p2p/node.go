package p2p

import (
	"net"
	"fmt"
	"math/rand"
	"sort"
)

const NodeIdBits = 512

type NodeID [NodeIdBits / 8]byte

func (i NodeID) String() string {
	return fmt.Sprintf("%x", i[:])
}

type Node struct {
	IP net.IP
	Port int
	ID NodeID
	Table [NodeIdBits]*Bucket
}

func NewNode(ip net.IP, port int, id NodeID) *Node {
	return &Node{
		IP: ip,
		Port: port,
		ID: id,
	}
}

func (n *Node) addr() *net.UDPAddr {
	return &net.UDPAddr{
		IP: n.IP,
		Port: n.Port,
	}
}

func (n *Node) setAddr(a *net.UDPAddr) {
	n.IP = a.IP
	n.Port = a.Port
}

func (n *Node) addrEqual(a *net.UDPAddr) bool {
	return n.IP.Equal(a.IP) && n.Port == a.Port
}

func (n *Node) Distance(p *Node) int {
	return calcDistance(n.ID, p.ID)
}

var matrix = [256]int{
	8, 7, 6, 6, 5, 5, 5, 5,
	4, 4, 4, 4, 4, 4, 4, 4,
	3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0,
}

func calcDistance(a, b NodeID) int {
	delta := 0
	for i := range a {
		x := a[i] ^ b[i]
		if x == 0 {
			delta += 8
		} else {
			delta += matrix[x]
			break
		}
	}

	return len(a) * 8 - delta
}

func findHashFromDistance(a NodeID, d int) NodeID {
	if d == 0 {
		return a
	}
	b := a

	pos := len(a) - d / 8 - 1
	bit := byte(0x01) << byte(d % 8) - 1
	if bit == 0 {
		pos++
		bit = 0x80
	}
	b[pos] = a[pos]&^bit | ^a[pos]&bit

	for i := pos + 1; i < len(a); i++ {
		b[pos] = byte(rand.Intn(255))
	}

	return b
}

// KAD algorithm
const K = 16
type Bucket [K]*Node

func (b *Bucket) Length() int {
	for i, node := range b {
		if node == nil {
			return i
		}
	}

	return 0
}

func (b *Bucket) Check(n *Node) bool {
	i := 0
	for _, node := range b {
		if node.ID == n.ID {
			break
		}
		i++
	}

	if i < len(b) {
		copy(b[1:], b[:i])
		b[0] = n
		return true
	}

	return false
}

func (b *Bucket) Remove(n *Node) {
	for i, node := range b {
		if node.ID == n.ID {
			copy(b[i:], b[i+1:])
			b[len(b) - 1] = nil
		}
	}
}

// the neighbors table
type Table struct {
	Buckets [NodeIdBits]*Bucket
	self *Node
}

func NewTable(n *Node) *Table {
	return &Table{
		self: n,
	}
}

func (t *Table) AddNode(n *Node) {
	if t.self.ID == n.ID {
		return
	}
	bucket := t.Buckets[t.self.Distance(n)]

	hasUpdated := bucket.Check(n)

	if !hasUpdated {
	// TODO check the first node in bucket, if it has disconneted,
	// replace the first node with n, else do nothing
	}
}

func (t *Table) Addnodes(nodes []*Node) {
	for _, node := range nodes {
		t.AddNode(node)
	}
}

func (t *Table) RemoveNode(n *Node) {
	if n.ID == t.self.ID {
		return
	}

	bucket := t.Buckets[t.self.Distance(n)]
	bucket.Remove(n)
}

func (t *Table) ClosestNodes(target NodeID, count int) *Closest {
	closet := &Closest{
		target: target,
	}

	for _, bucket := range t.Buckets {
		for _, node := range bucket {
			closet.Push(node, count)
		}
	}

	return closet
}

// Nodes closest to the target NodeID
type Closest struct {
	Nodes []*Node
	target NodeID
}

func (c *Closest) Push(n *Node, count int)  {
	length := len(c.Nodes)
	furtherNodeIndex := sort.Search(length, func(i int) bool {
		return calcDistance(c.Nodes[i].ID, c.target) > calcDistance(n.ID, c.target)
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

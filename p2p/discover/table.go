package discover

import "sort"

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

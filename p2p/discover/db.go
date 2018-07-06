package discover

import (
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"encoding/binary"
	"bytes"
	"os"
	"github.com/syndtr/goleveldb/leveldb/util"
	"time"
	"crypto/rand"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type NodeDB struct {
	db *leveldb.DB
	ID NodeID
	init sync.Once
	closed chan struct{}
}

const (
	dbVersion = "version"
	dbPrefix = "n:"
	dbDiscover = "discover"
	dbPing = dbDiscover + ":ping"
	dbPong = dbDiscover + ":pong"
	dbFindFail = dbDiscover + ":fail"
)

var (
	dbCleanInterval = time.Hour
	dbNodeLifetime = 24 * time.Hour
)

func newDB(path string, version int, id NodeID) (*NodeDB, error) {
	if path == "" {
		return newMemDB(id)
	} else {
		return newFileDB(path, version, id)
	}
}
func newMemDB(id NodeID) (*NodeDB, error) {
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		return nil, err
	}
	return &NodeDB{
		db: db,
		ID: id,
		closed: make(chan struct{}),
	}, nil
}
func newFileDB(path string, version int, id NodeID) (*NodeDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if _, ok := err.(*errors.ErrCorrupted); ok {
		db, err = leveldb.RecoverFile(path, nil)
	}
	if err != nil {
		return nil, err
	}

	vBytes := encodeVarint(int64(version))
	oldVBytes, err := db.Get([]byte(dbVersion), nil)

	if err == leveldb.ErrNotFound {
		err = db.Put([]byte(dbVersion), vBytes, nil)
		if err != nil {
			db.Close()
			return nil, err
		}
	} else if err == nil {
		if bytes.Equal(oldVBytes, vBytes) {
			return &NodeDB{
				db: db,
				ID: id,
				closed: make(chan struct{}),
			}, err
		} else {
			db.Close()
			err = os.RemoveAll(path)
			if err != nil {
				return nil, err
			}
			return newFileDB(path, version, id)
		}
	}
	return nil, err
}

func genKey(id NodeID, field string) []byte {
	nilID := NodeID{}
	if id == nilID {
		return []byte(field)
	}
	bytes := append([]byte(dbPrefix), id[:]...)
	return append(bytes, field...)
}

func parseKey(key []byte) (id NodeID, field string) {
	if bytes.HasPrefix(key, []byte(dbPrefix)) {
		rest := key[len(dbPrefix):]
		idLength := len(id)
		copy(id[:], rest[:idLength])
		return id, string(key[idLength:])
	}
	return NodeID{}, string(key)
}

func decodeVarint(varint []byte) int64 {
	i, n := binary.Varint(varint)
	if n <= 0 {
		return 0
	}
	return i
}
func encodeVarint(i int64) []byte {
	bytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(bytes, i)
	return bytes[:n]
}

func (db *NodeDB) retrieveInt64(key []byte) int64 {
	data, err := db.db.Get(key, nil)
	if err != nil {
		return 0
	}
	return decodeVarint(data)
}

func (db *NodeDB) storeInt64(key []byte, i int64) error {
	data := encodeVarint(i)
	return db.db.Put(key, data,nil)
}

func (db *NodeDB) retrieveNode(ID NodeID) *Node {
	data, err := db.db.Get(genKey(ID, dbDiscover), nil)
	if err != nil {
		return nil
	}

	node := new(Node)
	err = node.Deserialize(data)
	if err != nil {
		return nil
	}
	return node
}

func (db *NodeDB) updateNode(node *Node) error {
	key := genKey(node.ID, dbDiscover)
	data, err := node.Serialize()
	if err != nil {
		return err
	}
	return db.db.Put(key, data, nil)
}
// remove all data about the specific NodeID
func (db *NodeDB) deleteNode(ID NodeID) error {
	iterator := db.db.NewIterator(util.BytesPrefix(genKey(ID, "")), nil)
	defer iterator.Release()

	for iterator.Next() {
		err := db.db.Delete(iterator.Key(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *NodeDB) cleanStaleNodes() error {
	threshold := time.Now().Add(-dbNodeLifetime)
	it := db.db.NewIterator(nil, nil)
	defer it.Release()

	for it.Next() {
		id, field := parseKey(it.Key())
		if field != dbDiscover || bytes.Equal(id[:], db.ID[:]) {
			continue
		}
		
		connectTime := db.connectTime(id)
		if connectTime.After(threshold) {
			continue
		}
		db.deleteNode(id)
	}

	return nil
}

func (db *NodeDB) cleanRegularly() {
	ticker := time.NewTicker(dbCleanInterval)
	defer ticker.Stop()

	for {
		select {
		case <- ticker.C:
			db.cleanStaleNodes()
		case <- db.closed:
			return
		}
	}
}

func (db *NodeDB) randomNodes(length int, duration time.Duration) []*Node {
	iterator := db.db.NewIterator(nil, nil)
	defer iterator.Release()

	nodes := make([]*Node, 0, length)
	maxRounds := length * 5

	id := NodeID{}
	var node *Node

	for i := 0; len(nodes) < length && i < maxRounds; i++ {
		h := id[0]
		rand.Read(id[:])
		id[0] = h + id[0] % 16
		node = netxNode(iterator)
		if contains(nodes, node) {
			continue
		}
		nodes = append(nodes, node)
	}

	return nodes
}

func (db *NodeDB) connectTime(id NodeID) time.Time {
	key := genKey(id, dbPong)
	return time.Unix(db.retrieveInt64(key), 0)
}

func (db *NodeDB) initClean() {
	db.init.Do(func() {
		go db.cleanRegularly()
	})
}

func (db *NodeDB) close() {
	db.db.Close()
	close(db.closed)
}

// helper functions
func contains(nodes []*Node, node *Node) bool {
	for _, n := range nodes {
		if n.ID == node.ID {
			return true
		}
	}
	return false
}

func netxNode(iterator iterator.Iterator) (node *Node) {
	var field string
	var data []byte
	var err error

	for iterator.Next() {
		_, field = parseKey(iterator.Key())

		if field != dbDiscover {
			continue
		}

		data = iterator.Value()
		err = node.Deserialize(data)

		if err != nil {
			continue
		}
	}

	return node
}

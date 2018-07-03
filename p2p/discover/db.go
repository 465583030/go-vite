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
	//todo decode []byte to Node
	return node
}

func (db *NodeDB) updateNode(node *Node) error {
	key := genKey(node.ID, dbDiscover)
	// todo encode Node to []byte
	var data []byte
	return db.db.Put(key, data, nil)
}

func (db *NodeDB) deleteNode(ID NodeID) error {
	iterator := db.db.NewIterator(util.BytesPrefix(genKey(ID, "")), nil)
	for iterator.Next() {
		err := db.db.Delete(iterator.Key(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *NodeDB) close() {
	db.db.Close()
	close(db.closed)
}

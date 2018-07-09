package discover

import (
	"net"
	"fmt"
	"math/rand"
	"github.com/vitelabs/go-vite/p2p/protos"
	"github.com/golang/protobuf/proto"
	"errors"
	"github.com/vitelabs/go-vite/crypto"
	"net/url"
	"strconv"
	"encoding/hex"
	"strings"
	"math"
	"time"
)

const NodeURLScheme = "vnode"

const NodeIdBits = 512
const hashBits = 256

// NodeID section
type NodeID [NodeIdBits / 8]byte

func (i NodeID) String() string {
	return fmt.Sprintf("%x", i[:])
}

func HexStr2NodeID(str string) (NodeID, error) {
	var id NodeID
	bytes, err := hex.DecodeString(strings.TrimPrefix(str, "0x"))
	if err != nil {
		return id, err
	}
	if len(bytes) != len(id) {
		return id, fmt.Errorf("unmatch length, needs %d hex chars.", len(id) * 2)
	}
	copy(id[:], bytes)
	return id, nil
}

// Node section
type Hash [hashBits / 8]byte

type Node struct {
	IP net.IP
	TCP uint16
	UDP uint16
	ID NodeID
	hash Hash
	addTime time.Time
}

func NewNode(ip net.IP, tcp, udp uint16, id NodeID) *Node {
	var hash Hash
	copy(hash[:], crypto.Hash(32, id[:]))

	return &Node{
		IP: ip,
		TCP: tcp,
		UDP: udp,
		ID: id,
		hash: hash,
	}
}

func (n *Node) HasIp() bool {
	return n.IP != nil
}

func (n *Node) Validate() error {
	if !n.HasIp() {
		return errors.New("must has ip.")
	}
	if n.IP.IsMulticast() || n.IP.IsUnspecified() {
		return errors.New("invalid ip.")
	}
	if n.TCP == 0 {
		return errors.New("must has tcp port.")
	}
	if n.UDP == 0 {
		return errors.New("must has udp port.")
	}
	//todo validate n.ID
	return nil
}

// marshal node to url-like string which looks like:
// vnode://<hex node id>@<ip>:<tcp>?udp=<udp>
func (n *Node) String() string {
	nodeURL := url.URL{
		Scheme: NodeURLScheme,
	}
	if n.HasIp() {
		addr := net.TCPAddr{
			IP: n.IP,
			Port: int(n.TCP),
		}
		nodeURL.User = url.User(n.ID.String())
		nodeURL.Host = addr.String()
		if n.UDP != n.TCP {
			nodeURL.RawQuery = "udp=" + strconv.Itoa(int(n.UDP))
		}
	} else {
		nodeURL.Host = n.ID.String()
	}
	return nodeURL.String()
}

func (n *Node) Serialize() ([]byte, error) {
	nodepb := &protos.Node{
		IP: n.IP.String(),
		TCP: uint32(n.TCP),
		UDP: uint32(n.UDP),
	}
	return proto.Marshal(nodepb)
}

func (n *Node) Deserialize(bytes []byte) error {
	nodepb := &protos.Node{}
	err := proto.Unmarshal(bytes, nodepb)
	if err != nil {
		return err
	}
	n.IP = net.ParseIP(nodepb.IP)
	n.TCP = uint16(nodepb.TCP)
	return nil
}

func (n *Node) addr() *net.UDPAddr {
	return &net.UDPAddr{
		IP: n.IP,
		Port: int(n.UDP),
	}
}

func (n *Node) setAddr(a *net.UDPAddr) {
	n.IP = a.IP
	n.UDP = uint16(a.Port)
}

func (n *Node) addrEqual(a *net.UDPAddr) bool {
	return n.IP.Equal(a.IP) && int(n.TCP) == a.Port
}

func (n *Node) Distance(p *Node) int {
	return calcDistance(n.hash, p.hash)
}

// bytes xor to distance mapping table
var matrix = [256]int{
	0, 1, 2, 2, 3, 3, 3, 3,
	4, 4, 4, 4, 4, 4, 4, 4,
	5, 5, 5, 5, 5, 5, 5, 5,
	5, 5, 5, 5, 5, 5, 5, 5,
	6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8,
}

// xor every byte of a and b from left to right.
// stop at the first different byte (for brevity, we call it FDB).
// distance of a and b is bits-count of the FDB plus the bits-count of rest bytes.
func calcDistance(a, b Hash) int {
	delta := 0
	var i int
	for i := range a {
		x := a[i] ^ b[i]
		if x != 0 {
			delta += matrix[x]
			break
		}
	}

	return delta + (len(a) - i - 1) * 8
}

func disCmp(target, a, b Hash) int {
	var cmp byte
	for i := range target {
		cmp = a[i] ^ target[i] - b[i] ^ target[i]
		if cmp > 0 {
			return 1
		}
		if cmp < 0 {
			return -1
		}
	}

	return 0
}

func findHashFromDistance(a Hash, d int) Hash {
	if d == 0 {
		return a
	}
	b := a

	// pos mean the FDB between a and b from left to right.
	pos := len(a) - d / 8 - 1

	var xor byte = byte(d % 8)
	// mean the xor of FDB is greater or equal 127.
	if xor == 0 {
		pos++
		xor = byte(randInt(127, 256))
	} else {
		xor = expRand(xor)
	}
	// if byte1 xor byte2 get d,
	// then byte2 can be calc from (byte1^d | ^byte1&d)
	b[pos] = a[pos]&^xor | ^a[pos]&xor

	// fill the rest bytes.
	for i := pos + 1; i < len(a); i++ {
		b[i] = byte(rand.Intn(255))
	}

	return b
}

func randInt(min, max int) int {
	return rand.Intn(max - min) + min
}

// get rand int in [2**(n-1), 2**n)
func expRand(n byte) byte {
	low, up := int(math.Pow(2.0, float64(n - 1))), int(math.Pow(2.0, float64(n)))
	return byte(randInt(low, up))
}

// parse a url-like string to Node
func ParseNode(u string) (*Node, error) {
	var id NodeID
	var tcp, udp uint16
	var ip net.IP

	nodeURL, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	if nodeURL.Scheme != NodeURLScheme {
		return nil, errors.New(`invalid scheme, should be "vnode"`)
	}
	if nodeURL.User == nil {
		return nil, errors.New("missing node id.")
	}
	if id, err = HexStr2NodeID(nodeURL.User.String()); err != nil {
		return nil, fmt.Errorf("invalid node id. %v", err)
	}

	host, port, err := net.SplitHostPort(nodeURL.Host)
	if err != nil {
		return nil, fmt.Errorf("invalid host. %v", err)
	}
	if ip = net.ParseIP(host); ip == nil {
		return nil, errors.New("invalid ip.")
	}

	if i64, err := strconv.ParseUint(port, 10, 16); err != nil {
		return nil, fmt.Errorf("invalid port. ", err)
	} else {
		tcp = uint16(i64)
	}

	udpPort := nodeURL.Query().Get("udp")
	if udpPort != "" {
		if i64, err := strconv.ParseUint(udpPort, 10, 16); err != nil {
			return nil, fmt.Errorf("invalid udp port in query. ", err)
		} else {
			udp = uint16(i64)
		}
	} else {
		udp = tcp
	}

	return NewNode(ip, tcp, udp, id), nil
}

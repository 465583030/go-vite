// Package p2p implements the vite P2P network

package p2p

import (
	"time"
	"golang.org/x/crypto/ed25519"
)

const (
	dialTimeout = 10 * time.Second
)

type ServerConfig struct {
	// mandatory, `PrivateKey` must be set
	PrivateKey *ed25519.PrivateKey

	// `MaxPeers` is the maximum number of peeers that can be connected.
	MaxPeers int

	// `MaxPendingPeers` is the maximum number of peers that wait to be connected.
	MaxPendingPeers int

	Name string

	BootNodes []string

	// the port server will listen for incoming connections
	Port int
}

type Server struct {
	Config ServerConfig
	running bool
}

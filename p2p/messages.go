package p2p

import (
	"github.com/golang/protobuf/proto"
	"github.com/vitelabs/go-vite/p2p/msgs"
	"crypto/sha256"
	"bytes"
	"errors"
)

const (
	pingMsg = iota + 1
	pongMsg
)

type MsgReader interface {
	ReadMsg() (msgs.Msg, error)
}

type MsgWriter interface {
	WriteMsg(msg *msgs.Msg) error
}

type MsgReadWriter interface {
	MsgReader
	MsgWriter
}


func Unmarshal(msg proto.Message, originData []byte) error {
	var message *msgs.Msg
	err := proto.Unmarshal(originData, message)
	if err != nil {
		return err
	}

	checksum := message.Header.Checksum
	payload := message.Payload
	hash := sha256.Sum256(payload)

	if bytes.Compare(checksum, hash[:5]) != 0 {
		return errors.New("invalid payload.")
	}

	err = proto.Unmarshal(payload, msg)

	return err
}


func Send(pipe MsgWriter, command uint32, payload proto.Message) error {
	data, err := proto.Marshal(payload)
	if err != nil {
		return nil
	}

	hash := sha256.Sum256(data)
	checksum := hash[:5]
	msg := &msgs.Msg{
		Header: &msgs.Header{
			Domain: 1,
			Command: command,
			Checksum: checksum,
		},
		Payload: data,
	}

	pipe.WriteMsg(msg)
	return nil
}

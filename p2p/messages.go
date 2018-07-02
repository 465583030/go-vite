package p2p

import (
	"github.com/golang/protobuf/proto"
	"io"
)


type MsgReader interface {
	ReadMsg(*proto.Message) error
}

type MsgWriter interface {
	WriteMsg(*proto.Message) error
}

type MsgReadWriter interface {
	MsgReader
	MsgWriter
}


func Unmarshal(msg proto.Message, originData []byte) error {
	err := proto.Unmarshal(originData, msg)
	if err != nil {
		return err
	}

	// TODO verify msg.
	return nil
}

func Send(pipe io.Writer, msg proto.Message) error {
	// TODO generate msg checksum.

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil
	}
	pipe.Write(data)
	return nil
}

package p2p

import (
	"github.com/golang/protobuf/proto"
	"io"
)

type Message struct {

}

type Reader interface {
	Read(*Message) error
}

type Writer interface {
	Write(*Message) error
}

type ReadWriter interface {
	Reader
	Writer
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


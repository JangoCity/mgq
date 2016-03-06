package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	l4g "code.google.com/p/log4go"
)

const (
	HeadLength    = 4
	MaxBodyLength = 1024
)

type MgqProtocolImpl struct {
	buff []byte
	tokens []string
}

func MakePacket(bytes []byte) (buff []byte, err error) {

	bodyLength := len(bytes)
	if bodyLength > MaxBodyLength {
		l4g.Error("packet's size %d is too large than %d", bodyLength, MaxBodyLength)
		err = fmt.Errorf("packet's size %d is too large than %d", bodyLength, MaxBodyLength)
		return
	}

	buff = make([]byte, HeadLength+len(bytes))
	binary.BigEndian.PutUint32(buff[0:HeadLength], uint32(len(bytes)))

	copy(buff[HeadLength:], bytes)
	return
}

func (mp *MgqProtocolImpl) ReadBytes(conn *net.TCPConn) (res []byte, err error) {

	defer func() {
		if x := recover(); x != nil {
			var st = func(all bool) string {
				buf := make([]byte, 512)

				for {
					size := runtime.Stack(buf, all)
					if size == len(buf) {
						buf = make([]byte, len(buf)<<1)
						continue
					}
					break
				}

				return string(buf)
			}
			l4g.Error(st(false))
		}

	}()

	lengthBytes := make([]byte, HeadLength)
	if _, err = io.ReadFull(conn, lengthBytes); err != nil {
		l4g.Error("err:%s", err)
		return
	}

	var length int
	if length = int(binary.BigEndian.Uint32(lengthBytes)); length > MaxBodyLength {
		l4g.Error("the size of packet %d is larger than the limit %d", length, MaxBodyLength)
		err = fmt.Errorf("the size of packet %d is larger than the limit %d", length, MaxBodyLength)
		return
	}

	buff := make([]byte, length)
	if _, err = io.ReadFull(conn, buff); err != nil {
		l4g.Error("err:%s", err)
		return
	}

	return buff, nil
}

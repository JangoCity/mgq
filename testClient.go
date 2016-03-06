package main

import (
	"github.com/iamyh/mgq/service"
	"log"
	"net"
	"strconv"
	//"time"
)

func main() {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:22201")
	if err != nil {
		log.Println("err when new tcpAddr:", err)
		return
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Println("err when new conn:", err)
		return
	}

	ep := &service.MgqProtocolImpl{}
	for i := 1; i < 1000; i++ {
		buff, err := service.MakePacket([]byte("set queue " + strconv.Itoa(i)))
		if err != nil {
			log.Println("make packet err:", err)
			continue
		}

		conn.Write(buff)
		bytes, err := ep.ReadBytes(conn)
		if err == nil {
			log.Println("sever reply:", string(bytes))
		} else {
			log.Println("server err:", err)
		}

		buff, err = service.MakePacket([]byte("get queue"))
		if err != nil {
			log.Println("make packet err:", err)
			continue
		}

		conn.Write(buff)
		bytes, err = ep.ReadBytes(conn)
		if err == nil {
			log.Println("sever reply:", string(bytes))
		} else {
			log.Println("server err:", err)
		}

	}

	conn.Close()
}

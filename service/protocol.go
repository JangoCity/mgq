package service

import (
	"net"
	"runtime"
	l4g "code.google.com/p/log4go"
	"bufio"
	"errors"
	"strings"
	"github.com/iamyh/mgq/bdb"
	"fmt"
)

type MgqProtocolImpl struct {
}

func (mp *MgqProtocolImpl) ReadBytes(conn *net.TCPConn) (rsp []byte, err error) {

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

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		line := scanner.Text()
		var tokens []string
		tokens, err = Tokenize([]byte(line))
		if err != nil {
			l4g.Debug("Tokenize err:%s", err)
			return
		}

		if tokens == nil || len(tokens) <= 0 {
			l4g.Error("not found command to read")
		} else if len(tokens) >= 2 && strings.ToLower(tokens[0]) == "set" {
			if scanner.Scan() {
				value := scanner.Text()
				rsp, err = mp.processSet(tokens[1], value)
			}

		} else if len(tokens) >= 2 && (strings.ToLower(tokens[0]) == "get" || strings.ToLower(tokens[0]) == "gets") {
			rsp, err = mp.processGetC(tokens[1], 0)
		} else if len(tokens) >= 1 && strings.ToLower(tokens[0]) == "stats" {
			rsp = []byte(mp.processStats())
		} else if len(tokens) >= 1 && strings.ToLower(tokens[0]) == "quit" {
			rsp, err = mp.processQuit()
		} else if len(tokens) >= 1 && strings.ToLower(tokens[0]) == "delete" {
			rsp, err = mp.processDelete(tokens[1])
		} else {
			rsp = []byte("ERROR\r\n")
		}

	}

	return
}

func (mp *MgqProtocolImpl) processGetC(key string, position uint32) (rsp []byte, err error){

	item, err := bdb.DbGet(key, position)
	var suffixBytes []byte
	if err != nil {
		if err.Error() != bdb.DB_ERR_NOTFOUND.Error() {
			l4g.Error("DbGet err:%s", err)
			return
		}
		err = nil
		suffixBytes = []byte("END")
	} else {
		res := item.Data
		queueName := strings.Split(key, bdb.QUEUE_NAME_DELIMITER)[0]
		rsp = []byte(fmt.Sprintf("VALUE %s %d %d\r\n", queueName, 0, len(res)))
		rsp = append(rsp, res...)
		suffixBytes = []byte("\r\nEND")
	}

	rsp = append(rsp, suffixBytes...)
	rsp = append(rsp, "\r\n"...)
	return
}

func (mp *MgqProtocolImpl) processSet(key, value string) (rsp []byte, err error){

	item := bdb.DbItem{
		Data:[]byte(value),
	}

	err = bdb.DbSet(key, item)
	if err != nil {
		rsp = []byte(err.Error())
		return
	}

	rsp = []byte("STORED\r\n")
	return
}

func (mp *MgqProtocolImpl) processStats() (rsp []byte){

	rsp = []byte(bdb.GetAllCursorInfo() + "\r\n")
	return
}

func (mp *MgqProtocolImpl) processDelete(key string) (rsp []byte, err error){

	err = bdb.DelCursor(key)
	if err == nil {
		rsp = []byte("DELETED\r\n")
	} else {
		rsp = []byte("ERROR\r\n")
	}

	return
}

func (mp *MgqProtocolImpl) processQuit() (rsp []byte, err error){
	err = errors.New("quit")
	return
}

func Tokenize(buff []byte) (tokens []string, err error){
	if buff == nil || len(buff) == 0 {
		err = errors.New("not found command to tokenize")
		return
	}

	tokens = make([]string, 0)

	var value = strings.TrimSpace(string(buff))
	var newBuff = []byte(value)
	var start = 0
	for k, c := range newBuff {
		if string(c) == " " {
			tokens = append(tokens, string(newBuff[start:k]))
			start = k + 1
		}

	}
	tokens = append(tokens, string(newBuff[start:]))
	return
}

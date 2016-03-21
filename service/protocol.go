package service

import (
	"bufio"
	l4g "code.google.com/p/log4go"
	"errors"
	"fmt"
	"github.com/YoungPioneers/mgq/bdb"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	blockedQueueMap       = make(map[string]*Notify)
	blockedQueueMapRWLock = sync.RWMutex{}
)

type Notify struct {
	c      chan bool
	count  int
	locker sync.Mutex
}

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
		tokens, tErr := Tokenize([]byte(line))
		if tErr != nil {
			l4g.Debug("Tokenize err:%s", err)
		} else {
			if tokens == nil || len(tokens) <= 0 {
				l4g.Error("not found command to read")
			} else {
				lenTokens := len(tokens)
				cmd := strings.ToLower(tokens[0])
				if lenTokens >= 2 && cmd == "set" || cmd == "setn" {
					if scanner.Scan() {
						value := scanner.Text()
						if cmd == "set" {
							rsp, err = mp.processSet(tokens[1], value)
						} else if cmd == "setn" {
							rsp, err = mp.processSetN(tokens[1], value)
						}
					}
				} else if lenTokens >= 2 && (cmd == "get" || cmd == "gets" || cmd == "getc" || cmd == "getn") {

					if cmd == "getn" {
						timeout := 0
						if lenTokens == 3 {
							timeout, _ = strconv.Atoi(tokens[2])
						}

						rsp, err = mp.processGetN(tokens[1], time.Duration(timeout))
					} else {
						cursor := 0
						if lenTokens == 3 {
							cursor, _ = strconv.Atoi(tokens[2])
						}
						rsp, err = mp.processGetC(tokens[1], uint32(cursor))
					}

				} else if lenTokens >= 1 && cmd == "stats" {
					rsp = []byte(mp.processStats())
				} else if lenTokens >= 1 && cmd == "quit" {
					rsp, err = mp.processQuit()
				} else if lenTokens >= 1 && cmd == "delete" {
					rsp, err = mp.processDelete(tokens[1])
				}
			}
		}
	}

	if rsp == nil || len(rsp) == 0 {
		rsp = []byte("ERROR\r\n")
	}
	return
}

func (mp *MgqProtocolImpl) processGetC(key string, position uint32) (rsp []byte, err error) {

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

func (mp *MgqProtocolImpl) processGetN(key string, timeout time.Duration) (rsp []byte, err error) {

	item, err := bdb.DbGet(key, 0)
	var suffixBytes []byte
	if err != nil {
		if err.Error() != bdb.DB_ERR_NOTFOUND.Error() {
			l4g.Error("DbGet err:%s", err)
			return
		}
		err = nil
		suffixBytes = []byte("END")
		queueName := strings.Split(key, bdb.QUEUE_NAME_DELIMITER)[0]
		blockedQueueMapRWLock.RLock()
		var notify *Notify
		var ok bool
		if notify, ok = blockedQueueMap[queueName]; !ok {
			blockedQueueMapRWLock.RUnlock()
			blockedQueueMapRWLock.Lock()
			notify = &Notify{
				count: 0,
				c:     make(chan bool),
			}
			blockedQueueMap[queueName] = notify
			blockedQueueMapRWLock.Unlock()
		} else {
			blockedQueueMapRWLock.RUnlock()
		}

		notify.locker.Lock()
		notify.count++
		notify.locker.Unlock()

		if timeout == 0 {
			<-notify.c
		} else {
			timeTicker := time.Tick(timeout * time.Second)
			select {
			case <-timeTicker:
				//timeout
				l4g.Debug("timeout for break block")
				return
			case <-notify.c:

			}
		}

		notify.locker.Lock()
		notify.count--
		notify.locker.Unlock()

		rsp, err = mp.processGetC(key, 0)
	} else {
		res := item.Data
		queueName := strings.Split(key, bdb.QUEUE_NAME_DELIMITER)[0]
		rsp = []byte(fmt.Sprintf("VALUE %s %d %d\r\n", queueName, 0, len(res)))
		rsp = append(rsp, res...)
		suffixBytes = []byte("\r\nEND")
		rsp = append(rsp, suffixBytes...)
		rsp = append(rsp, "\r\n"...)
	}

	return
}

func (mp *MgqProtocolImpl) processSet(key, value string) (rsp []byte, err error) {

	item := bdb.DbItem{
		Data: []byte(value),
	}

	err = bdb.DbSet(key, item)
	if err != nil {
		rsp = []byte(err.Error())
		return
	}

	rsp = []byte("STORED\r\n")
	return
}

func (mp *MgqProtocolImpl) processSetN(key, value string) (rsp []byte, err error) {

	item := bdb.DbItem{
		Data: []byte(value),
	}

	err = bdb.DbSet(key, item)
	if err != nil {
		rsp = []byte(err.Error())
		return
	}

	queueName := strings.Split(key, bdb.QUEUE_NAME_DELIMITER)[0]
	blockedQueueMapRWLock.RLock()
	var notify *Notify
	var ok bool
	if notify, ok = blockedQueueMap[queueName]; !ok {
		blockedQueueMapRWLock.RUnlock()
		blockedQueueMapRWLock.Lock()
		notify = &Notify{
			count: 0,
			c:     make(chan bool),
		}
		blockedQueueMap[queueName] = notify
		blockedQueueMapRWLock.Unlock()
	} else {
		blockedQueueMapRWLock.RUnlock()
	}

	notify.locker.Lock()
	for i := 0; i < notify.count; i++ {
		notify.c <- true
	}
	notify.locker.Unlock()

	rsp = []byte("STORED\r\n")
	return
}

func (mp *MgqProtocolImpl) processStats() (rsp []byte) {

	rsp = []byte(bdb.GetAllCursorInfo() + "\r\n")
	return
}

func (mp *MgqProtocolImpl) processDelete(key string) (rsp []byte, err error) {

	err = bdb.DelCursor(key)
	if err == nil {
		rsp = []byte("DELETED\r\n")
	} else {
		rsp = []byte("ERROR\r\n")
	}

	return
}

func (mp *MgqProtocolImpl) processQuit() (rsp []byte, err error) {
	err = errors.New("quit")
	return
}

func Tokenize(buff []byte) (tokens []string, err error) {
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

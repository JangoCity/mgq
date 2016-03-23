package benchmark 

import (
	"fmt"
	"time"
	"testing"
	"github.com/bradfitz/gomemcache/memcache"
	"sync"
	"strconv"
)

const (
	itemNum = 50 * 10000
)
/*
func Test_MgqMessageMiss(t *testing.T) {
	mc := memcache.New("127.0.0.1:22201")
	mc.Timeout = 1 * time.Second
	var item *memcache.Item
	var setCounter int64
	var getCounter int64
	var setFailCounter int64
	var getFailCounter int64
	var err error
	var setStartTime int64
	var setEndTime int64
	var getStartTime int64
	var getEndTime int64

	setCounter = 0
	setStartTime = time.Now().UnixNano()
	for {
		item = &memcache.Item{
			Key:   "queue",
			Value: []byte("i love iyouya.com"),
		}
		err := mc.Set(item)
		if err != nil {
			fmt.Println(err)
			setFailCounter++
			continue
		}

		setCounter++
		if setCounter >= itemNum {
			break
		}
	}

	setEndTime = time.Now().UnixNano()

	getStartTime = time.Now().UnixNano()
	var i int64
	for i = 1; i <= setCounter; i++ {
		item, err = mc.Get("queue")
		if err != nil {
			fmt.Println(err)
			getFailCounter++
			continue
		}
		getCounter++
	}

	getEndTime = time.Now().UnixNano()

	fmt.Printf("mgq message set total:%d, cost total time:%+v ns, %d per/s ,fail set total:%d\n", setCounter, setEndTime - setStartTime, (setCounter * 1000 * 1000 * 1000) / (setEndTime - setStartTime), setFailCounter)
	fmt.Printf("mgq message get total:%d, cost total time:%+v ns, %d per/s ,fail get total:%d\n", getCounter, getEndTime - getStartTime, (getCounter * 1000 * 1000 * 1000) / (getEndTime - getStartTime) , getFailCounter)
}
*/
func Benchmark_MgqSingleSetAndGet(b *testing.B) {
	mc := memcache.New("127.0.0.1:22201")
	mc.Timeout = 1 * time.Second
	var item *memcache.Item
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item = &memcache.Item{
			Key:   "queue",
			Value: []byte("i love iyouya.com"),
		}
		err := mc.Set(item)
		if err != nil {
			fmt.Println(err)
		}

		item, err = mc.Get("queue")
		if err != nil || item == nil || item.Value == nil {
			fmt.Println(err)
		}

	}
}

func Benchmark_MgqMultiSetAndGet(b *testing.B) {
	var wg sync.WaitGroup

	b.ResetTimer()
	fn := func (queue string) {
		defer wg.Done()
		mc := memcache.New("127.0.0.1:22201")
		mc.Timeout = 1 * time.Second
		var item *memcache.Item
		for i := 0; i < b.N; i++ {
			item = &memcache.Item{
				Key:   queue,
				Value: []byte("i love iyouya.com"),
			}
			err := mc.Set(item)
			if err != nil {
				fmt.Println(err)
				continue
			}

			item, err = mc.Get(queue)
			if err != nil || item == nil || item.Value == nil {
				fmt.Println(err)
			}
		}
	}

	for i := 0; i < 4; i ++ {
		go fn("queue" + strconv.Itoa(i))
		wg.Add(1)
	}

	wg.Wait()
}

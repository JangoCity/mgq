package main

import (
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"time"
)

func main() {
	mc := memcache.New("127.0.0.1:22201")
	start := time.Now().UnixNano()
	for i := 0; i < 20000; i++ {
		item := &memcache.Item{
			Key:   "queue",
			Value: []byte("wo shi shui"),
		}
		item, err := mc.Get("queue")
		if err == nil {
			fmt.Println(string(item.Value))
		} else {
			fmt.Println(err)
		}
	}
	end := time.Now().UnixNano()
	fmt.Println(uint64(end - start))
}

package main

import (
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
)

func main() {
	mc := memcache.New("127.0.0.1:22201")
	item := &memcache.Item{
		Key:   "queue",
		Value: []byte("iyouya.com's queue"),
	}
	err := mc.Set(item)
	if err != nil {
		fmt.Println(err)
		return
	}

	item, err = mc.Get("queue")
	if err == nil {
		fmt.Println(string(item.Value))
	} else {
		fmt.Println(err)
	}

}

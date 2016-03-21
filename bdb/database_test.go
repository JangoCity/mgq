package bdb

import (
	"fmt"
	"testing"
)

func Test_A(t *testing.T) {

	for i := 0; i <= 1000; i++ {
		item := DbItem{
			Data: []byte{'g', 'o', 'l', 'a', 'n', 'g'},
		}

		err := DbSet("myqueue", item)
		if err != nil {
			t.Fatal(err)
		}

		var newItem DbItem
		newItem, err = DbGet("myqueue", int64(i))
		if err != nil {
			t.Fatal(err)
		}

		//_ = newItem
		fmt.Println(string(newItem.Data))
	}

}

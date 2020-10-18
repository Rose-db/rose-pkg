package rose

import (
	"fmt"
	"testing"
)

func TestConcurrentInsertsAndReads(t *testing.T) {
	var r *Rose
	num := 100000
	c := make(chan string, num)

	defer testRemoveFileSystemDb(t)

	r = testCreateRose(testGetTestName(t))

	produce := func(c chan string, id string) {
		_, err := r.Insert(&Metadata{
			Id:  id,
			Data: []uint8(fmt.Sprintf("value-%s", id)),
		})

		if err != nil {
			t.Errorf("Rose: Invalid insert with error: %s", err.Error())

			panic(err)
		}

		c<- id
	}

	consume := func(id string) {
		res, err := r.Read(&Metadata{
			Id:  id,
		})

		if err != nil {
			t.Errorf("Rose: Invalid delete with error: %s", err.Error())

			panic(err)
		}

		if res.Status != FoundResultStatus {
			t.Errorf("%s: Rose::Read Invalid read status. Got %s, Expected %s", testGetTestName(t), res.Status, FoundResultStatus)
		}
	}

	for i := 0; i < num; i++ {
		id := fmt.Sprintf("id-%d", i)

		go produce(c, id)
	}

	curr := 0
	for a := range c {
		consume(a)
		curr++

		if curr == num {
			break
		}
	}

	r.Shutdown()
}

func TestConcurrentDelete(t *testing.T) {
	var r *Rose

	num := 100000
	c := make(chan string, num)

	defer testRemoveFileSystemDb(t)

	r = testCreateRose(testGetTestName(t))

	produce := func(c chan string, id string) {
		_, err := r.Insert(&Metadata{
			Id:  id,
			Data: []uint8(fmt.Sprintf("value-%s", id)),
		})

		if err != nil {
			t.Errorf("Rose: Invalid insert with error: %s", err.Error())

			panic(err)
		}

		c<- id
	}

	consume := func(id string) {
		res, err := r.Delete(&Metadata{
			Id:  id,
		})

		if err != nil {
			t.Errorf("Rose: Invalid delete with error: %s", err.Error())

			panic(err)
		}

		if res.Status != EntryDeletedStatus {
			t.Errorf("%s: Rose::Delete Invalid delete status. Got %s, Expected %s", testGetTestName(t), res.Status, EntryDeletedStatus)
		}
	}

	for i := 0; i < num; i++ {
		id := fmt.Sprintf("id-%d", i)

		go produce(c, id)
	}

	curr := 0
	for a := range c {
		consume(a)
		curr++

		if curr == num {
			break
		}
	}

	r.Shutdown()
}


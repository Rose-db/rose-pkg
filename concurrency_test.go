package rose

import (
	"fmt"
	"testing"
)

func TestConcurrentInsertsAndReads(t *testing.T) {
	var a *Rose
	var m *Metadata

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	num := 100000
	idChan := make(chan [2]string, num)

	for i := 0; i < num; i++ {
		go func(i int, idChan chan [2]string) {
			s := []uint8(fmt.Sprintf("id-value-%d", i))
			id := fmt.Sprintf("id-%d", i)

			m = &Metadata{
				Data:   s,
				Id:     id,
			}

			appResult, appErr := a.Insert(m)

			assertSuccessfulInsertResult(appErr, appResult, t)

			idChan<- [2]string{id, string(s)}
		}(i, idChan)
	}

	for i := 0; i < num; i++ {
		c := <-idChan

		res, err := a.Read(&Metadata{
			Id:  c[0],
		})

		assertSuccessfulReadResult(err, res, t)

		if res.Result != c[1] {
			t.Errorf("%s: Rose::Read Invalid result. Got %s, Expected %s", testGetTestName(t), res.Result, c[1])

			return
		}
	}

	a.Shutdown()
}

func TestConcurrentDelete(t *testing.T) {
	t.Skip()
	var r *Rose

	num := 10
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


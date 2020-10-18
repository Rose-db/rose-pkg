package rose

import (
	"fmt"
	"testing"
)

func TestInternalMemoryDbInsert(t *testing.T) {
	m := newMemoryDb()

	testInsertFixture(m,10000)

	// since block index starts at 0, expected must be 3
	assertInternalDbValues(m, 3, 0, t)
	assertInternalDbIntegrity(m, 10000, 4, t)
}

func TestInternalDbDelete(t *testing.T) {
	m := newMemoryDb()

	ids := testInsertFixture(m,10000)

	// since block index starts at 0, expected must be 3
	assertInternalDbValues(m, 3, 0, t)
	assertInternalDbIntegrity(m, 10000, 4, t)

	for _, id := range ids {
		m.Delete(id)
	}

	assertInternalDbValues(m, 3, 10000, t)
	assertInternalDbIntegrity(m, 0, 4, t)
}

func TestInternalDbDeleteReAllocation(t *testing.T) {
	m := newMemoryDb()

	ids := testInsertFixture(m,10000)

	// since block index starts at 0, expected must be 3
	assertInternalDbValues(m, 3, 0, t)
	assertInternalDbIntegrity(m, 10000, 4, t)

	for _, id := range ids {
		m.Delete(id)
	}

	assertInternalDbValues(m, 3, 10000, t)
	assertInternalDbIntegrity(m, 0, 4, t)

	ids = testInsertFixture(m,50000)

	assertInternalDbValues(m, 16, 0, t)
	assertInternalDbIntegrity(m, 50000, 17, t)
}

func testInsertFixture(m *memDb, num int) []string {
	ids := []string{}
	for i := 0; i < num; i++ {
		id := fmt.Sprintf("id-%d", i)
		ids = append(ids, id)
		value := []uint8("sdkfjsdjfsadfjklsajdfkčl")
		m.Insert(id, &value)
	}

	return ids
}

func assertInternalDbValues(m *memDb, expectedMapIdx uint16, freeListLen int, t *testing.T) {
	if m.CurrMapIdx != expectedMapIdx {
		t.Errorf("%s: Invalid CurrMapIdx. Got %d, Expected %d", testGetTestName(t), m.CurrMapIdx, expectedMapIdx)

		return
	}

	if len(m.FreeIdsList) != freeListLen {
		t.Errorf("%s: Invalid FreeIdsList length. Got %d, Expected %d", testGetTestName(t), len(m.FreeIdsList), 0)

		return
	}
}

func assertInternalDbIntegrity(m *memDb, expectedLen int, expectedCapacity int, t *testing.T) {
	if len(m.InternalDb) != expectedCapacity {
		t.Errorf("%s: Invalid InternalDb length. Got %d, Expected %d", testGetTestName(t), len(m.InternalDb), expectedCapacity)

		return
	}

	fullNum := 0

	for _, list := range m.InternalDb {
		for _, a := range list {
			if a != nil {
				fullNum++
			}
		}
	}

	if fullNum != expectedLen {
		t.Errorf("%s: Invalid InternalDb internal map length. Got %d, Expected %d", testGetTestName(t), fullNum, expectedLen)
	}

	if len(m.IdLookupMap) != expectedLen {
		t.Errorf("%s: Invalid IdLookupMap. Got %d, Expected %d", testGetTestName(t), fullNum, expectedLen)
	}
}
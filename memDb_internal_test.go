package rose

import (
	"fmt"
	"testing"
)

func TestInternalMemoryDb(t *testing.T) {
	m := newMemoryDb()

	ids := []string{}
	for i := 0; i < 3000; i++ {
		id := fmt.Sprintf("id-%d", i)
		ids = append(ids, id)
		value := []uint8("sdkfjsdjfsadfjklsajdfkčl")
		m.Insert(id, &value)
	}

	if m.CurrMapIdx != 1 {
		t.Errorf("%s: Invalid CurrMapIdx. Got %d, Expected %d", testGetTestName(t), m.CurrMapIdx, 1)
		t.Errorf("%s: Invalid InternalDb length. Got %d, Expected %d", testGetTestName(t), len(m.InternalDb), 3000)
	}

	// not part of the test, just deleting for next round of testing
	for _, id := range ids {
		m.Delete(id)
	}

	num := 100000
	for i := 0; i < num; i++ {
		id := fmt.Sprintf("id-%d", i)
		value := []uint8("sdkfjsdjfsadfjklsajdfkčl")
		m.Insert(id, &value)
	}

	// this is safe to do since division is rounding down
	expected := uint64(num / 3000) + 1

	if m.CurrMapIdx != expected {
		t.Errorf("%s: Invalid CurrMapIdx. Got %d, Expected %d", testGetTestName(t), m.CurrMapIdx, expected)
		t.Errorf("%s: Invalid InternalDb length. Got %d, Expected %d", testGetTestName(t), len(m.InternalDb), num)
	}
}
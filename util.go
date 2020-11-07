package rose

import "encoding/json"

func splitMetadataArray(mArr []*Metadata, size int) [][]*Metadata {
	min := func(a, b int) int {
		if a <= b {
			return a
		}

		return b
	}

	var batch [][]*Metadata = [][]*Metadata{}

	for i := 0; i < len(mArr); i += size {
		b := mArr[i:min(i+size, len(mArr))]

		batch = append(batch, b)
	}

	return batch
}

func removeElem(s []uint64, i int) []uint64 {
	// s[i] goes to the end, while the element that was in s[i] is replaced with the last element
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	// just return everything except the last element since that is where s[i] is now
	return s[:len(s)-1]
}

func prepareData(id string, data []uint8) *[]uint8 {
	i := id + delim + string(data) + "\n"
	a := []uint8(i)

	return &a
}

func isJSON(s []uint8) bool {
	var js json.RawMessage
	return json.Unmarshal(s, &js) == nil
}

func appendByte(slice []uint8, data ...uint8) []uint8 {
	m := len(slice)
	n := m + len(data)
	if n > cap(slice) { // if necessary, reallocate
		// allocate double what's needed, for future growth.
		newSlice := make([]uint8, (n+1)*2)
		copy(newSlice, slice)
		slice = newSlice
	}
	slice = slice[0:n]
	copy(slice[m:n], data)
	return slice
}

package rose

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
	cpp := &data

	// create the string to be saved as a single row on fs
	*cpp = append(*cpp, uint8(10))
	b := []uint8(id + " ")
	*cpp = append(b, *cpp...)

	return cpp
}

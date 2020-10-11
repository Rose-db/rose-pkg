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

func removeElem(s []int, i int) []int {
	// s[i] goes to the end, while the element that was in s[i] is replaced with the last element
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	// just return everything except the last element since that is where s[i] is now
	return s[:len(s)-1]
}

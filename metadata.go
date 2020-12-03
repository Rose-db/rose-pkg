package rose

type WriteMetadata struct {
	CollectionName string
	Data []uint8
}

type ReadMetadata struct {
	CollectionName string
	ID int
	Data interface{}
}

type DeleteMetadata struct {
	CollectionName string
	ID int
}

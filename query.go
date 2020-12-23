package rose

type Link struct {
	Collection string
	Field string
	Value string
}

type Query struct {
	Collections []string
	Link []Link
	Return []string
}
package rose

/**
Creates the next id to be generated. It only generates id <= idFactory.Max.
How these ids are represented in the database is handled by the Database
struct. After it reaches idFactory.Max, it resets to 0.
*/
type blockIdFactory struct {
	Max uint16
	CurrIdx uint16
}

func newBlockIdFactory() *blockIdFactory {
	return &blockIdFactory{
		Max: 2999,
		CurrIdx: 0,
	}
}

func (m *blockIdFactory) Next() uint16 {
	if m.CurrIdx == 0 {
		m.CurrIdx++

		return uint16(0)
	}

	c := m.CurrIdx
	m.CurrIdx++

	if m.CurrIdx > 2999 {
		m.CurrIdx = 0
	}

	return c
}


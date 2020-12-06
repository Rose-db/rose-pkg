package rose

import "sync"

type indexDb struct {
	// map of user supplied ids to InternalDb indexes
	// IdLookupMap::string -> idx::uint -> InternalDb[idx] -> []uint8
	sync.RWMutex

	WriteDriver *fsDriver
	ReadDriver *fsDriver
	DeleteDriver *fsDriver
}

package rose

import (
	"encoding/json"
	"sync"
)

type dbReadResult struct {
	Idx uint16
	Id string
	Result interface{}
}

/**
	A database is built with an Database.InternalDb that hold the values of
	the database in a key value map. Key is a uint while value is a []uint8 ([]byte).

	Individual indexes are stored in Database.IdLookupMap as a key value pair
	of user supplied key as a string and the index as uint.

    Database.InternalDb is growing in size as more values are stored. These size
	increaments can be called blocks.
	Blocks can hold up to 3000 indexes (value). When the reach max size, a new map
	is created with the same size.
 */
type Db struct {
	InternalDb map[uint16]*[3000]*[]uint8
	// map of user supplied ids to InternalDb indexes
	// IdLookupMap::string -> idx::uint -> InternalDb[idx] -> []uint8
	IdLookupMap map[string][2]uint16
	FreeIdsList map[string][2]uint16
	idFactory *idFactory
	RWMutex *sync.RWMutex

	CurrMapIdx uint16

	FsDriver *fsDriver
}

func newMemoryDb(fsDriver *fsDriver) *Db {
	d := &Db{
		FsDriver: fsDriver,
	}

	d.init()

	return d
}

/**
	- A RW lock is acquired
	- New uint idx is generated by idFactory
 	- idx is stored in Database.IdLookupMap
	- a check is made for the current block
		- if the block does not exist, a new block is created
	- the value is stored in the block with its index
*/
func (d *Db) Write(id string, v []uint8, fsWrite bool) (int, Error) {
	d.RWMutex.Lock()

	if len(d.FreeIdsList) > 0 {
		list := d.FreeIdsList[id]
		idx := list[0]
		mapId := list[1]

		d.IdLookupMap[id] = [2]uint16{idx, mapId}
		// we know that the block has to exist since its in the free list
		// and that means it was deleted
		m := d.InternalDb[mapId]

		m[idx] = &v

		delete(d.FreeIdsList, id)

		d.RWMutex.Unlock()

		return FreeListQueryStatus, nil
	}

	var idx uint16
	var m *[3000]*[]uint8

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		d.RWMutex.Unlock()

		return ExistsStatus, nil
	}

	// r/w operation, create uint64 index
	idx = d.idFactory.Next()

	m, created := d.getBlock()

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{idx, d.CurrMapIdx}

	if fsWrite {
		err := d.saveOnFs(id, v)

		if err != nil {
			return 0, err
		}
	}

	// saving the pointer address of the data, not the actual data
	m[idx] = &v

	if idx == 2999 {
		d.CurrMapIdx++
	}

	d.RWMutex.Unlock()

	if created {
		return NewBlockCreatedStatus, nil
	}
	
	return NormalExecutionStatus, nil
}

func (d *Db) Delete(id string) (bool, Error) {
	d.RWMutex.Lock()

	var idData [2]uint16
	var mapId, idx uint16
	var m *[3000]*[]uint8

	idData, ok := d.IdLookupMap[id]

	if !ok {
		d.RWMutex.Unlock()

		return false, nil
	}

	mapId = idData[1]

	a := []uint8(id)
	err := d.deleteFromFs(&a, mapId)

	if err != nil {
		return false, err
	}

	idx = idData[0]

	// get the map where the id value is
	m = d.InternalDb[mapId]

	delete(d.IdLookupMap, id)
	m[idx] = nil

	d.FreeIdsList[id] = [2]uint16{idx, mapId}

	d.RWMutex.Unlock()

	return true, nil
}

func (d *Db) Read(id string, v interface{}) *dbReadResult {
	d.RWMutex.Lock()

	var m *[3000]*[]uint8
	var idData [2]uint16
	var mapId, idx uint16
	var b *[]uint8

	idData, ok := d.IdLookupMap[id]

	idx = idData[0]
	mapId = idData[1]

	if !ok {
		d.RWMutex.Unlock()

		return nil
	}

	// get the map where the id value is
	m = d.InternalDb[mapId]

	// get the value of id, value is a pointer, not the actual data
	b = m[idx]

	d.RWMutex.Unlock()

	e := json.Unmarshal(*b, v)

	if e != nil {
		panic(e)
	}

	return &dbReadResult{
		Idx:    idx,
		Id:     id,
		Result: v,
	}
}

func (d *Db) Shutdown() Error {
	d.init()

	return d.FsDriver.Shutdown()
}

/**
	PRIVATE METHOD. DO NOT USE IN CLIENT CODE

	Save the data on the filesystem
 */
func (d *Db) saveOnFs(id string, v []uint8) Error {
	jobs := []*job{
		{Entry: prepareData(id, v)},
	}

	return d.FsDriver.Save(&jobs, d.CurrMapIdx)
}

func (d *Db) deleteFromFs(id *[]uint8, mapIdx uint16) Error {
	jobs := []*job{
		{Entry: id},
	}

	return d.FsDriver.MarkDeleted(&jobs, mapIdx)
}

/**
	PRIVATE METHOD. DO NOT USE IN CLIENT CODE

	Returns an existing memory block if exists. If not, creates a new one and returns it
 */
func (d *Db) getBlock() (*[3000]*[]uint8, bool) {
	// check if the current block exists or need to be created
	m, ok := d.InternalDb[d.CurrMapIdx]

	if !ok {
		// current block does not exist, created a new one
		m = &[3000]*[]uint8{}
		d.InternalDb[d.CurrMapIdx] = m

		return m, true
	}

	return m, false
}

func (d *Db) init() {
	d.InternalDb = make(map[uint16]*[3000]*[]uint8)
	d.InternalDb[0] = &[3000]*[]uint8{}
	d.RWMutex = &sync.RWMutex{}
	d.FreeIdsList = make(map[string][2]uint16)

	d.IdLookupMap = make(map[string][2]uint16)

	d.idFactory = newIdFactory()
	d.CurrMapIdx = 0
}

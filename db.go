package rose

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
)

type dbReadResult struct {
	Idx    uint16
	ID    int
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
type db struct {
	InternalDb map[uint16]*[3000]*[]uint8
	// map of user supplied ids to InternalDb indexes
	// IdLookupMap::string -> idx::uint -> InternalDb[idx] -> []uint8
	IdLookupMap          map[int][2]uint16
	Index                map[int]int64
	AutoIncrementCounter int
	CurrMapIdx           uint16
	BlockIdFactory       *blockIdFactory
	sync.RWMutex

	WriteDriver *fsDriver
	ReadDriver *fsDriver
	DeleteDriver *fsDriver
}

func newDb(write *fsDriver, read *fsDriver, delete *fsDriver) *db {
	d := &db{
		WriteDriver: write,
		ReadDriver: read,
		DeleteDriver: delete,
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
func (d *db) Write(data []uint8) (int, int, Error) {
	d.Lock()

	id := d.AutoIncrementCounter

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		d.Unlock()

		return 0, 0, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf( "ID integrity validation. Duplicate ID %d found. This should not happen. Try this write again", id),
		}
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{uint16(d.AutoIncrementCounter % blockMark), d.CurrMapIdx}

	bytesWritten, size, err := d.saveOnFs(id, data)
	offset := size - bytesWritten

	d.Index[id] = offset

	if err != nil {
		d.Unlock()

		return 0, 0, err
	}

	blockIdx := d.BlockIdFactory.Next()

	if blockIdx == 2999 {
		d.CurrMapIdx++
	}

	d.AutoIncrementCounter += 1

	d.Unlock()

	return NormalExecutionStatus, id, nil
}

func (d *db) Delete(id int) (bool, Error) {
	d.Lock()

	var idData [2]uint16
	var mapId uint16

	idData, ok := d.IdLookupMap[id]

	if !ok {
		d.Unlock()

		return false, nil
	}

	idx, ok := d.Index[id]

	if !ok {
		d.Unlock()

		return false, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("Index integrity violation. Index for ID %d does not exist. Please, restart Rose and try again", id),
		}
	}

	mapId = idData[1]

	err := d.deleteFromFs(id, mapId, idx)

	if err != nil {
		d.Unlock()

		return false, err
	}

	delete(d.IdLookupMap, id)
	delete(d.Index, id)

	d.Unlock()

	return true, nil
}

func (d *db) Read(id int, data interface{}) *dbReadResult {
	idData, ok := d.IdLookupMap[id]

	if !ok {
		return nil
	}

	idx := idData[0]
	mapId := idData[1]

	if !ok {
		return nil
	}

	index, _ := d.Index[id]

	b, err := d.ReadDriver.Read(index, mapId)

	if err != nil {
		return nil
	}

	e := json.Unmarshal(*b, data)

	if e != nil {
		panic(e)
	}

	return &dbReadResult{
		Idx:    idx,
		ID:     id,
		Result: data,
	}
}

// shutdown does not do anything for now until I decide what to do with multiple drivers
func (d *db) Shutdown() [3]Error {
	d.init()

	errors := [3]Error{}

	if err := d.WriteDriver.Shutdown(); err != nil {
		errors[0] = err
	}

	if err := d.ReadDriver.Shutdown(); err != nil {
		errors[1] = err
	}

	if err := d.DeleteDriver.Shutdown(); err != nil {
		errors[2] = err
	}

	return errors
}

func (d *db) writeIndex(id int, mapIdx uint16, offset int64) Error {
	d.Lock()

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		d.Unlock()

		return nil
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{uint16(d.AutoIncrementCounter % blockMark), mapIdx}

	d.Index[id] = offset

	d.AutoIncrementCounter += 1

	d.Unlock()

	return nil
}

func (d *db) writeOnDefragmentation(id int, v []uint8, mapIdx uint16) Error {
	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		return nil
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{uint16(d.AutoIncrementCounter % blockMark), mapIdx}

	_, _, err := d.WriteDriver.Save(prepareData(id, v), mapIdx)

	if err != nil {
		return err
	}

	return nil
}

/**
PRIVATE METHOD. DO NOT USE IN CLIENT CODE

Save the data on the filesystem
*/
func (d *db) saveOnFs(id int, v []uint8) (int64, int64, Error) {
	return d.WriteDriver.Save(prepareData(id, v), d.CurrMapIdx)
}

func (d *db) deleteFromFs(id int, mapIdx uint16, idx int64) Error {
	idStr := strconv.Itoa(id)
	idByte := []uint8(idStr)
	idPtr := &idByte

	return d.DeleteDriver.MarkStrategicDeleted(idPtr, []uint8(delMark), mapIdx, idx)
}

func (d *db) init() {
	d.InternalDb = make(map[uint16]*[3000]*[]uint8)
	d.InternalDb[0] = &[3000]*[]uint8{}
	d.Index = make(map[int]int64)
	d.AutoIncrementCounter = 0
	d.BlockIdFactory = newBlockIdFactory()

	d.IdLookupMap = make(map[int][2]uint16)

	d.CurrMapIdx = 0
}

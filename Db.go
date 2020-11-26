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
type Db struct {
	InternalDb map[uint16]*[3000]*[]uint8
	// map of user supplied ids to InternalDb indexes
	// IdLookupMap::string -> idx::uint -> InternalDb[idx] -> []uint8
	IdLookupMap          map[int][2]uint16
	Index                map[int]int64
	AutoIncrementCounter int
	CurrMapIdx           uint16
	sync.RWMutex

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
func (d *Db) Write(data []uint8, fsWrite bool) (int, int, Error) {
	id := d.AutoIncrementCounter
	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		return 0, 0, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("ID integrity validation. Duplicate ID found. This should not happen. Try this write again"),
		}
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{uint16(d.AutoIncrementCounter % blockMark), d.CurrMapIdx}

	if fsWrite {
		bytesWritten, size, err := d.saveOnFs(id, data)
		offset := size - bytesWritten

		d.Index[id] = offset

		if err != nil {
			return 0, 0, err
		}
	}

	if d.AutoIncrementCounter != 0 && d.AutoIncrementCounter % blockMark == 0 {
		d.CurrMapIdx++
	}

	d.AutoIncrementCounter += 1

	return NormalExecutionStatus, id, nil
}

func (d *Db) GoWrite(data []uint8, fsWrite bool, goRes chan *GoAppResult) {
	d.Lock()

	id := d.AutoIncrementCounter

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		res := &GoAppResult{
			Result: nil,
			Err: &systemError{
				Code:    DbIntegrityViolationCode,
				Message: "ID integrity validation. Duplicate ID found. This should not happen. Try this write again",
			},
		}

		d.Unlock()

		goRes <- res

		return
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{uint16(d.AutoIncrementCounter % blockMark), d.CurrMapIdx}

	if fsWrite {
		bytesWritten, size, err := d.saveOnFs(id, data)

		d.Index[id] = size - bytesWritten

		if err != nil {
			res := &GoAppResult{
				Result: nil,
				Err: &systemError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Unable to save document to the filesystem. Underlying message is: '%s'", err.Error()),
				},
			}

			d.Unlock()

			goRes <- res

			return
		}
	}

	if d.AutoIncrementCounter != 0 && d.AutoIncrementCounter % blockMark == 0 {
		d.CurrMapIdx++
	}

	d.AutoIncrementCounter += 1

	res := &GoAppResult{
		Result: &AppResult{
			ID:   id,
			Method: WriteMethodType,
			Status: OkResultStatus,
			Reason: "",
		},
		Err: nil,
	}

	d.Unlock()

	goRes <- res
}

func (d *Db) Delete(id int) (bool, Error) {
	var idData [2]uint16
	var mapId uint16

	idData, ok := d.IdLookupMap[id]

	if !ok {
		return false, nil
	}

	mapId = idData[1]

	err := d.deleteFromFs(id, mapId)

	if err != nil {
		return false, err
	}

	delete(d.IdLookupMap, id)
	delete(d.Index, id)

	return true, nil
}

func (d *Db) GoDelete(id int, resChan chan *GoAppResult) {
	d.Lock()

	var idData [2]uint16
	var mapId uint16

	idData, ok := d.IdLookupMap[id]

	if !ok {
		d.Unlock()

		resChan <- &GoAppResult{
			Result: nil,
			Err: &dataError{
				Code:    DataErrorCode,
				Message: fmt.Sprintf("Document under uuid %d does not exist", id),
			},
		}

		return
	}

	mapId = idData[1]

	err := d.deleteFromFs(id, mapId)

	if err != nil {
		d.Unlock()

		resChan <- &GoAppResult{
			Result: nil,
			Err:    err,
		}

		return
	}

	delete(d.IdLookupMap, id)
	delete(d.Index, id)

	d.Unlock()

	resChan <- &GoAppResult{
		Result: &AppResult{
			ID:   id,
			Method: DeleteMethodType,
			Status: DeletedResultStatus,
			Reason: "",
		},
		Err: nil,
	}
}

func (d *Db) Read(id int, data interface{}) *dbReadResult {
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

	b, err := d.FsDriver.Read(index, mapId)

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

func (d *Db) Shutdown() Error {
	d.init()

	if err := d.FsDriver.Shutdown(); err != nil {
		return err
	}

	d.FsDriver = nil

	return nil
}

func (d *Db) writeOnLoad(id int, mapIdx uint16, lock *sync.RWMutex, offset int64) Error {
	lock.Lock()

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		lock.Unlock()

		return nil
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{uint16(d.AutoIncrementCounter % blockMark), mapIdx}

	d.Index[id] = offset

	d.AutoIncrementCounter += 1

	lock.Unlock()

	return nil
}

func (d *Db) writeOnDefragmentation(id int, v []uint8, mapIdx uint16) Error {
	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		return nil
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{uint16(d.AutoIncrementCounter % blockMark), mapIdx}

	_, _, err := d.FsDriver.Save(prepareData(id, v), mapIdx)

	if err != nil {
		return err
	}

	return nil
}

/**
PRIVATE METHOD. DO NOT USE IN CLIENT CODE

Save the data on the filesystem
*/
func (d *Db) saveOnFs(id int, v []uint8) (int64, int64, Error) {
	return d.FsDriver.Save(prepareData(id, v), d.CurrMapIdx)
}

func (d *Db) deleteFromFs(id int, mapIdx uint16) Error {
	idx, _ := d.Index[id]

	idStr := strconv.Itoa(id)
	idByte := []uint8(idStr)
	idPtr := &idByte

	return d.FsDriver.MarkStrategicDeleted(idPtr, mapIdx, idx)
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
	d.Index = make(map[int]int64)
	d.AutoIncrementCounter = 0

	d.IdLookupMap = make(map[int][2]uint16)

	d.CurrMapIdx = 0
}

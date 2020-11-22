package rose

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
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
	Index map[string]int64
	idFactory *idFactory
	CurrMapIdx uint16
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
func (d *Db) Write(v []uint8, fsWrite bool) (int, string,  Error) {
	id := uuid.New().String()

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		return ExistsStatus, "", nil
	}

	var idx uint16
	var m *[3000]*[]uint8

	// r/w operation, create uint64 index
	idx = d.idFactory.Next()

	m, created := d.getBlock()

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{idx, d.CurrMapIdx}

	if fsWrite {
		bytesWritten, size, err := d.saveOnFs(id, v)

		d.Index[id] = size - bytesWritten

		if err != nil {
			return 0, "", err
		}
	}

	// saving the pointer address of the data, not the actual data
	m[idx] = &v

	if idx == 2999 {
		d.CurrMapIdx++
	}

	if created {
		return NewBlockCreatedStatus, id,  nil
	}
	
	return NormalExecutionStatus, id, nil
}

func (d *Db) GoWrite(v []uint8, fsWrite bool, goRes chan *GoAppResult) {
	d.Lock()

	id := uuid.New().String()

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		res := &GoAppResult{
			Result: nil,
			Err:    &systemError{
				Code:    DbIntegrityViolationCode,
				Message: "Uuid integrity validation. Duplicate uuid found. This should not happen. Try this write again",
			},
		}

		d.Unlock()

		goRes<- res

		return
	}

	var idx uint16
	var m *[3000]*[]uint8

	// r/w operation, create uint64 index
	idx = d.idFactory.Next()

	m, created := d.getBlock()

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{idx, d.CurrMapIdx}

	if fsWrite {
		bytesWritten, size, err := d.saveOnFs(id, v)

		d.Index[id] = size - bytesWritten

		if err != nil {
			res := &GoAppResult{
				Result: nil,
				Err:    &systemError{
					Code:    DbIntegrityViolationCode,
					Message: fmt.Sprintf("Unable to save document to the filesystem. Underlying message is: '%s'", err.Error()),
				},
			}

			d.Unlock()

			goRes<- res

			return
		}
	}

	// saving the pointer address of the data, not the actual data
	m[idx] = &v

	if idx == 2999 {
		d.CurrMapIdx++
	}

	if created {
		res := &GoAppResult{
			Result: &AppResult{
				Uuid:  id,
				Method: WriteMethodType,
				Status: OkResultStatus,
				Reason: "",
			},
			Err: nil,
		}

		d.Unlock()

		goRes<- res

		return
	}

	res := &GoAppResult{
		Result: &AppResult{
			Uuid:  id,
			Method: WriteMethodType,
			Status: OkResultStatus,
			Reason: "",
		},
		Err: nil,
	}

	d.Unlock()

	goRes<- res
}

func (d *Db) Delete(id string) (bool, Error) {
	var idData [2]uint16
	var mapId, idx uint16
	var m *[3000]*[]uint8

	idData, ok := d.IdLookupMap[id]

	if !ok {
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
	delete(d.Index, id)
	m[idx] = nil

	return true, nil
}

func (d *Db) GoDelete(id string, resChan chan *GoAppResult) {
	d.Lock()

	var idData [2]uint16
	var mapId, idx uint16
	var m *[3000]*[]uint8

	idData, ok := d.IdLookupMap[id]

	if !ok {
		d.Unlock()

		resChan<- &GoAppResult{
			Result: nil,
			Err:    &dataError{
				Code:    DataErrorCode,
				Message: fmt.Sprintf("Document under uuid %s does not exist", id),
			},
		}

		return
	}

	mapId = idData[1]

	a := []uint8(id)
	err := d.deleteFromFs(&a, mapId)

	if err != nil {
		d.Unlock()

		resChan<- &GoAppResult{
			Result: nil,
			Err:    err,
		}

		return
	}

	idx = idData[0]

	// get the map where the id value is
	m = d.InternalDb[mapId]

	delete(d.IdLookupMap, id)
	delete(d.Index, id)
	m[idx] = nil

	d.Unlock()

	resChan<- &GoAppResult{
		Result: &AppResult{
			Uuid:  id,
			Method: DeleteMethodType,
			Status: DeletedResultStatus,
			Reason: "",
		},
		Err:    nil,
	}
}

func (d *Db) Read(id string, v interface{}) *dbReadResult {
	idData, ok := d.IdLookupMap[id]

	idx := idData[0]
	mapId := idData[1]

	if !ok {
		return nil
	}

	index, _ := d.Index[id]

	b, err := d.FsDriver.Read(index, mapId)

	if err != nil {
		panic(err)
	}

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

func (d *Db) writeOnLoad(id string, v []uint8, mapIdx uint16, lock *sync.RWMutex, offset int64) Error {
	lock.Lock()

	var idx uint16
	var m *[3000]*[]uint8

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		lock.Unlock()

		return nil
	}

	// r/w operation, create uint64 index
	idx = d.idFactory.Next()

	m, ok := d.InternalDb[mapIdx]

	if !ok {
		// current block does not exist, created a new one
		m = &[3000]*[]uint8{}
		d.InternalDb[mapIdx] = m
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{idx, mapIdx}

	// saving the pointer address of the data, not the actual data
	m[idx] = &v
	d.Index[id] = offset

	lock.Unlock()

	return nil
}

func (d *Db) writeOnDefragmentation(id string, v []uint8, mapIdx uint16) Error {
	var idx uint16
	var m *[3000]*[]uint8

	// check if the entry already exists
	if _, ok := d.IdLookupMap[id]; ok {
		return nil
	}

	// r/w operation, create uint64 index
	idx = d.idFactory.Next()

	m, ok := d.InternalDb[mapIdx]

	if !ok {
		// current block does not exist, created a new one
		m = &[3000]*[]uint8{}
		d.InternalDb[mapIdx] = m
	}

	// r operation, add COMPUTED index to the index map
	d.IdLookupMap[id] = [2]uint16{idx, mapIdx}

	// saving the pointer address of the data, not the actual data
	m[idx] = &v

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
func (d *Db) saveOnFs(id string, v []uint8) (int64, int64, Error) {
	return d.FsDriver.Save(prepareData(id, v), d.CurrMapIdx)
}

func (d *Db) deleteFromFs(id *[]uint8, mapIdx uint16) Error {
	idx, ok := d.Index[string(*id)]

	if !ok {
		return d.FsDriver.MarkDeleted(id, mapIdx)
	}

	return d.FsDriver.MarkStrategicDeleted(id, mapIdx, idx)
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
	d.Index = make(map[string]int64)

	d.IdLookupMap = make(map[string][2]uint16)

	d.idFactory = newIdFactory()
	d.CurrMapIdx = 0
}

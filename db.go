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

type db struct {
	Index                map[int]int64
	AutoIncrementCounter int
	BlockTracker map[uint16][2]uint16
	Name string
	Balancer *balancer
	sync.RWMutex

	WriteDriver *fsDriver
	ReadDriver *fsDriver
	DeleteDriver *fsDriver
}

func newDb(write *fsDriver, read *fsDriver, delete *fsDriver, name string, blockNum uint16) *db {
	d := &db{
		WriteDriver: write,
		ReadDriver: read,
		DeleteDriver: delete,
		Name: name,
	}

	d.init()

	d.Balancer = newBalancer(blockNum)

	return d
}

func (d *db) Write(data []uint8) (int, int, Error) {
	d.Lock()

	id := d.AutoIncrementCounter
	d.AutoIncrementCounter += 1

	// check if the entry already exists
	if _, ok := d.Index[id]; ok {
		d.Unlock()

		return 0, 0, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf( "ID integrity validation. Duplicate ID %d found. This should not happen. Try this write again", id),
		}
	}

	mapId := d.getBlockId(id)

	bytesWritten, size, err := d.saveOnFs(id, data, mapId)

	if err != nil {
		return 0, 0, err
	}

	offset := size - bytesWritten

	d.Index[id] = offset

	track, ok := d.BlockTracker[mapId]

	if !ok {
		t := [2]uint16{}

		track = t
	}

	track[0] += 1

	d.BlockTracker[mapId] = track

	d.Balancer.reSpawnIfNeeded(uint16(len(d.BlockTracker)))

	d.Unlock()

	return NormalExecutionStatus, id, nil
}

func (d *db) Delete(id int) (bool, Error) {
	d.Lock()

	blockId := d.getBlockId(id)

	idx, ok := d.Index[id]

	if !ok {
		d.Unlock()

		return false, nil
	}

	delete(d.Index, id)

	err := d.deleteFromFs(id, blockId, idx)

	if err != nil {
		d.Unlock()

		return false, err
	}

	track := d.increaseBlockTracker(blockId)

	if track == defragmentMark {
		indexes, err := d.tryDefragmentation(blockId)

		if err != nil {
			d.Unlock()

			return false, err
		}

		for i, index := range indexes {
			d.Index[i] = index
		}

		if err := d.WriteDriver.reload(); err != nil {
			d.Unlock()

			return false, err
		}

		if err := d.ReadDriver.reload(); err != nil {
			d.Unlock()

			return false, err
		}

		if err := d.DeleteDriver.reload(); err != nil {
			d.Unlock()

			return false, err
		}

		d.resetBlockTracker(blockId)
	}

	d.Unlock()

	return true, nil
}

func (d *db) ReadStrategic(id int, data interface{}) (*dbReadResult, Error) {
	d.Lock()

	index, ok := d.Index[id]

	if !ok {
		d.Unlock()

		return nil, nil
	}

	mapId := d.getBlockId(id)

	b, err := d.ReadDriver.ReadStrategic(index, mapId)

	d.Unlock()

	if err != nil {
		return nil, &dbIntegrityError{
			Code:    DbIntegrityViolationCode,
			Message: fmt.Sprintf("An error occurred while trying to read driver: %s", err.Error()),
		}
	}

	e := json.Unmarshal(*b, data)

	if e != nil {
		return nil, &systemError{
			Code:    SystemErrorCode,
			Message: fmt.Sprintf("Cannot unmarshal JSON string. This can be a bug with Rose or an invalid document. Try deleting and write the document again. The underlying error is: %s", e.Error()),
		}
	}

	return &dbReadResult{
		ID:     id,
		Result: data,
	}, nil
}

/**
    1. Delete the document with the specified ID
    2. Write the new document into the same block
    3. Replace the previous index with the new one
 */
func (d *db) Replace(id int, data []uint8) Error {
	d.Lock()
	_, ok := d.Index[id]

	if !ok {
		d.Unlock()

		return nil
	}

	blockId := d.getBlockId(id)

	if err := d.unlockedDelete(id, blockId); err != nil {
		d.Unlock()

		return err
	}

	if err := d.unlockedWrite(id, data, blockId); err != nil {
		d.Unlock()

		return err
	}

	track := d.increaseBlockTracker(blockId)

	if track == defragmentMark {
		indexes, err := d.tryDefragmentation(blockId)

		if err != nil {
			d.Unlock()

			return err
		}

		for i, index := range indexes {
			d.Index[i] = index
		}

		if err := d.WriteDriver.reload(); err != nil {
			d.Unlock()

			return err
		}

		if err := d.ReadDriver.reload(); err != nil {
			d.Unlock()

			return err
		}

		if err := d.DeleteDriver.reload(); err != nil {
			d.Unlock()

			return err
		}

		d.resetBlockTracker(blockId)
	}

	d.Unlock()

	return nil
}

func (d *db) Query(q *QueryBuilder) ([]*QueryResult, Error) {
	stmt := q.IfStmt

	ch := make(chan *queueResponse)

	queryItem := &balancerRequest{
		BlockNum: uint16(len(d.BlockTracker)),
		Item: struct {
			CollName string
			Field    string
			Value    interface{}
			DataType dataType
		}{
			CollName: stmt.Equal.Collection,
			Field: stmt.Equal.Field,
			Value: stmt.Equal.Value,
			DataType: stmt.Equal.DataType,
		},
		Response: ch,
	}

	return d.Balancer.Push(queryItem)
}

// shutdown does not do anything for now until I decide what to do with multiple drivers
func (d *db) Shutdown() [3]Error {
	d.init()

	d.Balancer.Close()

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

func (d *db) writeIndex(id int, offset int64) Error {
	d.Lock()

	// check if the entry already exists
	if _, ok := d.Index[id]; ok {
		d.Unlock()

		return nil
	}

	d.Index[id] = offset

	d.AutoIncrementCounter += 1

	d.Unlock()

	return nil
}

func (d *db) writeOnDefragmentation(id int, v []uint8, mapIdx uint16) Error {
	// check if the entry already exists
	if _, ok := d.Index[id]; ok {
		return nil
	}

	_, _, err := d.WriteDriver.Save(prepareData(id, v), mapIdx)

	if err != nil {
		return err
	}

	return nil
}

func (d *db) unlockedDelete(id int, mapId uint16) Error {
	idx, ok := d.Index[id]

	if !ok {
		return nil
	}

	err := d.deleteFromFs(id, mapId, idx)

	if err != nil {
		return err
	}

	return nil
}

func (d *db) unlockedWrite(id int, data []uint8, mapId uint16) Error {
	// r operation, add COMPUTED index to the index map
	bytesWritten, size, err := d.saveOnFs(id, data, mapId)
	offset := size - bytesWritten

	d.Index[id] = offset

	if err != nil {
		return err
	}

	return nil
}

/**
PRIVATE METHOD. DO NOT USE IN CLIENT CODE

Save the data on the filesystem
*/
func (d *db) saveOnFs(id int, v []uint8, mapId uint16) (int64, int64, Error) {
	return d.WriteDriver.Save(prepareData(id, v), mapId)
}

func (d *db) deleteFromFs(id int, mapIdx uint16, idx int64) Error {
	idStr := strconv.Itoa(id)
	idByte := []uint8(idStr)

	return d.DeleteDriver.MarkStrategicDeleted(idByte, []uint8(delMark), mapIdx, idx)
}

func (d *db) getBlockId(id int) uint16 {
	return uint16(id / blockMark)
}

func (d *db) tryDefragmentation(blockId uint16) (map[int]int64, Error) {
	indexes, err := defragmentBlock(blockId, d.Name)

	if err != nil {
		return nil, err
	}

	return indexes, nil
}

func (d *db) increaseBlockTracker(blockId uint16) uint16 {
	track, _ := d.BlockTracker[blockId]

	track[1] += 1

	d.BlockTracker[blockId] = track

	return track[1]
}

func (d *db) resetBlockTracker(blockId uint16) {
	track, _ := d.BlockTracker[blockId]
	track[1] = 0

	d.BlockTracker[blockId] = track
}

func (d *db) init() {
	d.Index = make(map[int]int64)
	d.AutoIncrementCounter = 1
	d.BlockTracker = make(map[uint16][2]uint16)
}

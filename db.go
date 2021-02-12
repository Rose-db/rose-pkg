package rose

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type dbReadResult struct {
	Idx    uint16
	ID    int
	Result interface{}
}

type db struct {
	PrimaryIndex                map[int]int64
	
	AutoIncrementCounter int
	BlockTracker map[uint16][2]uint16
	DocCount map[uint16]int
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
		DocCount: make(map[uint16]int),
	}

	d.init()

	d.Balancer = newBalancer(blockNum)

	return d
}

func (d *db) Write(data interface{}) (int, int, Error) {
	d.Lock()

	id := d.AutoIncrementCounter
	d.AutoIncrementCounter += 1

	// check if the entry already exists
	if _, ok := d.PrimaryIndex[id]; ok {
		d.Unlock()

		return 0, 0, newError(DbIntegrityMasterErrorCode, IndexNotExistsCode, fmt.Sprintf( "ID integrity validation. Duplicate ID %d found. This should not happen. Try this write again", id))
	}

	mapId := d.getBlockId(id)

	bytesWritten, size, err := d.saveOnFs(id, data, mapId)

	if err != nil {
		return 0, 0, err
	}

	offset := size - bytesWritten

	d.PrimaryIndex[id] = offset

	track, ok := d.BlockTracker[mapId]

	if !ok {
		t := [2]uint16{}

		track = t
	}

	track[0] += 1

	d.BlockTracker[mapId] = track

	d.incrementDocCount(mapId)

	go func(bLen int, b *balancer) {
		b.reSpawnIfNeeded(uint16(bLen))
	}(len(d.BlockTracker), d.Balancer)

	d.Unlock()

	return NormalExecutionStatus, id, nil
}

func (d *db) BulkWrite(data []interface{}) (int, string, Error) {
	d.Lock()

	if len(data) == 0 {
		return NormalExecutionStatus, "", nil
	}

	written := ""
	for _, v := range data {
		id := d.AutoIncrementCounter
		d.AutoIncrementCounter += 1

		// check if the entry already exists
		if _, ok := d.PrimaryIndex[id]; ok {
			d.Unlock()

			return 0, "", newError(DbIntegrityMasterErrorCode, IndexNotExistsCode, fmt.Sprintf( "ID integrity validation. Duplicate ID %d found. This should not happen. Try this write again", id))
		}

		mapId := d.getBlockId(id)

		bytesWritten, size, err := d.saveOnFs(id, v, mapId)

		if err != nil {
			return 0, "", err
		}

		offset := size - bytesWritten

		d.PrimaryIndex[id] = offset

		track, ok := d.BlockTracker[mapId]

		if !ok {
			t := [2]uint16{}

			track = t
		}

		track[0] += 1

		d.BlockTracker[mapId] = track

		written += fmt.Sprintf("%d,", id)
	}

	written = strings.TrimRight(written, ",")

	go func(bLen int, b *balancer) {
		b.reSpawnIfNeeded(uint16(bLen))
	}(len(d.BlockTracker), d.Balancer)

	d.Unlock()

	return NormalExecutionStatus, written, nil
}

func (d *db) Delete(id int) (bool, Error) {
	d.Lock()

	blockId := d.getBlockId(id)

	idx, ok := d.PrimaryIndex[id]

	if !ok {
		d.Unlock()

		return false, nil
	}

	delete(d.PrimaryIndex, id)

	err := d.deleteFromFs(id, blockId, idx)

	if err != nil {
		d.Unlock()

		return false, err
	}

	d.Unlock()

	return true, nil
}

func (d *db) ReadStrategic(id int, data interface{}) (*dbReadResult, Error) {
	d.Lock()

	index, ok := d.PrimaryIndex[id]

	if !ok {
		d.Unlock()

		return nil, nil
	}

	mapId := d.getBlockId(id)

	b, err := d.ReadDriver.ReadStrategic(index, mapId)

	d.Unlock()

	if err != nil {
		return nil, err
	}

	e := json.Unmarshal(*b, data)

	if e != nil {
		return nil, newError(SystemMasterErrorCode, UnmarshalFailCode, fmt.Sprintf("Cannot unmarshal JSON string. This can be a bug with Rose or an invalid document. Try deleting and write the document again. The underlying error is: %s", e.Error()))
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
func (d *db) Replace(id int, data interface{}) Error {
	d.Lock()
	_, ok := d.PrimaryIndex[id]

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
			d.PrimaryIndex[i] = index
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

func (d *db) Query(singleQuery *singleQuery) ([]QueryResult, Error) {
	ch := make(chan *queueResponse)

	return d.Balancer.Push(&balancerRequest{
		CollName: singleQuery.collName,
		BlockNum: uint16(d.AutoIncrementCounter / blockMark + 1),
		OperationStages: singleQuery.stages,
		Response: ch,
	})
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
	if _, ok := d.PrimaryIndex[id]; ok {
		d.Unlock()

		return nil
	}

	d.PrimaryIndex[id] = offset

	d.AutoIncrementCounter += 1

	d.Unlock()

	return nil
}

func (d *db) writeOnDefragmentation(id int, v []uint8, mapIdx uint16) Error {
	// check if the entry already exists
	if _, ok := d.PrimaryIndex[id]; ok {
		return nil
	}

	_, _, err := d.WriteDriver.Save(prepareData(id, v), mapIdx)

	if err != nil {
		return err
	}

	return nil
}

func (d *db) unlockedDelete(id int, mapId uint16) Error {
	idx, ok := d.PrimaryIndex[id]

	if !ok {
		return nil
	}

	err := d.deleteFromFs(id, mapId, idx)

	if err != nil {
		return err
	}

	return nil
}

func (d *db) unlockedWrite(id int, data interface{}, mapId uint16) Error {
	// r operation, add COMPUTED index to the index map
	bytesWritten, size, err := d.saveOnFs(id, data, mapId)
	offset := size - bytesWritten

	d.PrimaryIndex[id] = offset

	if err != nil {
		return err
	}

	return nil
}

/**
PRIVATE METHOD. DO NOT USE IN CLIENT CODE

Save the data on the filesystem
*/
func (d *db) saveOnFs(id int, v interface{}, mapId uint16) (int64, int64, Error) {
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

func (d *db) incrementDocCount(mapId uint16) {
	if _, ok := d.DocCount[mapId]; !ok {
		d.DocCount[mapId] = 0
	}

	d.DocCount[mapId]++

	if len(d.DocCount) == 2 {
		delete(d.DocCount, mapId - 1)
	}
}

func (d *db) init() {
	d.PrimaryIndex = make(map[int]int64)
	d.AutoIncrementCounter = 1
	d.BlockTracker = make(map[uint16][2]uint16)
}

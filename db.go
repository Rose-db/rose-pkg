package rose

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fastjson"
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
	PrimaryIndex  map[int]int64
	FieldIndex map[string]*fieldIndex
	FieldIndexKeys []string

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
	}

	d.init()

	d.Balancer = newBalancer(blockNum)

	return d
}

// data interface{} is string
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

	idxVal := []uint8(data.(string))
	if err := d.validateFieldIndex(idxVal); err != nil {
		return 0, 0, err
	}

	if err := d.writeFieldIndexWithoutLock(offset, idxVal, mapId); err != nil {
		return 0, 0, err
	}

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

	e := json.Unmarshal(b.val, data)

	if e != nil {
		return nil, newError(SystemMasterErrorCode, UnmarshalFailCode, fmt.Sprintf("Cannot unmarshal JSON string. This can be a bug with Rose or an invalid document. Try deleting and write the document again. The underlying error is: %s", e.Error()))
	}

	return &dbReadResult{
		ID:     id,
		Result: data,
	}, nil
}

func (d *db) ReadBy(m ReadByMetadata) ([]*dbReadResult, Error) {
	d.Lock()

	fieldIndex := d.FieldIndex[m.Field]

	if fieldIndex.DataType != m.DataType {
		return nil, newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Validation error. Invalid data type. You provided %s but the index is a %s data type", string(m.DataType), string(fieldIndex.DataType)))
	}

	var p fastjson.Parser
	results := make([]*dbReadResult, 0)

	from := paginate(m.Pagination.Page)

	found := 0
	for i := from; i <= len(fieldIndex.Index) && found < m.Pagination.Limit; i++ {
		idx := fieldIndex.Index[i]

		b, err := d.ReadDriver.ReadStrategic(idx.Pos, idx.BlockId)

		if err != nil {
			return nil, err
		}

		v, e := p.ParseBytes(b.val)

		if e != nil {
			return nil, newError(DbIntegrityMasterErrorCode, UnmarshalFailCode, fmt.Sprintf("Unable to parse JSON from an already saved value. Be sure that what you saved is a JSON construct: %s", e.Error()))
		}

		if v.Exists(m.Field) {
			if m.DataType == stringIndexType && string(v.GetStringBytes(m.Field)) == m.Value.(string) {
				var data interface{}
				e := json.Unmarshal(b.val, &data)

				if e != nil {
					return nil, newError(SystemMasterErrorCode, UnmarshalFailCode, fmt.Sprintf("Cannot unmarshal JSON string. This can be a bug with Rose or an invalid document. Try deleting and write the document again. The underlying error is: %s", e.Error()))
				}

				results = append(results, &dbReadResult{
					ID:     b.id,
					Result: data,
				})

				found++
			}

			if m.DataType == intIndexType && v.GetInt(m.Field) == m.Value.(int) {
				var data interface{}
				e := json.Unmarshal(b.val, &data)

				if e != nil {
					return nil, newError(SystemMasterErrorCode, UnmarshalFailCode, fmt.Sprintf("Cannot unmarshal JSON string. This can be a bug with Rose or an invalid document. Try deleting and write the document again. The underlying error is: %s", e.Error()))
				}

				results = append(results, &dbReadResult{
					ID:     b.id,
					Result: data,
				})

				found++
			}

			if m.DataType == floatIndexType && v.GetFloat64(m.Field) == m.Value.(float64) {
				var data interface{}
				e := json.Unmarshal(b.val, &data)

				if e != nil {
					return nil, newError(SystemMasterErrorCode, UnmarshalFailCode, fmt.Sprintf("Cannot unmarshal JSON string. This can be a bug with Rose or an invalid document. Try deleting and write the document again. The underlying error is: %s", e.Error()))
				}

				results = append(results, &dbReadResult{
					ID:     b.id,
					Result: data,
				})

				found++
			}

			if m.DataType == boolIndexType && v.GetBool(m.Field) == m.Value.(bool) {
				var data interface{}
				e := json.Unmarshal(b.val, &data)

				if e != nil {
					return nil, newError(SystemMasterErrorCode, UnmarshalFailCode, fmt.Sprintf("Cannot unmarshal JSON string. This can be a bug with Rose or an invalid document. Try deleting and write the document again. The underlying error is: %s", e.Error()))
				}

				results = append(results, &dbReadResult{
					ID:     b.id,
					Result: data,
				})

				found++
			}
		}
	}

	d.Unlock()

	return results, nil
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

func (d *db) validateFieldIndex(val []uint8) Error {
	var p fastjson.Parser

	pVal, pErr := p.ParseBytes(val)

	if pErr != nil {
		return newError(SystemMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Cannot write index. Data is unparsable. This might be a bug but if it is not, change your data: %s", pErr.Error()))
	}

	for _, fieldName := range d.FieldIndexKeys {
		if !pVal.Exists(fieldName) {
			return newError(SystemMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Cannot write index. Field name '%s' does not exist on provided JSON object. If you created an index on a JSON structure on a certain field and its data type, that field must exists with the correct underlying data type", fieldName))
		}
	}

	return nil
}

// no need to handle error since the index is validate with ::validateFieldIndex()
func (d *db) writeFieldIndexWithoutLock(offset int64, val []uint8, blockId uint16) Error {
	var p fastjson.Parser

	pVal, _ := p.ParseBytes(val)

	for fieldName, fieldIndex := range d.FieldIndex {
		var idxVal interface{}
		if fieldIndex.DataType == stringIndexType {
			idxVal = pVal.GetStringBytes(fieldName)
		} else if fieldIndex.DataType == intIndexType {
			idxVal = pVal.GetInt(fieldName)
		} else if fieldIndex.DataType == floatIndexType {
			idxVal = pVal.GetFloat64(fieldName)
		} else if fieldIndex.DataType == boolIndexType {
			idxVal = pVal.GetBool(fieldName)
		}

		fieldIndex.Add(offset, idxVal, blockId)
	}

	return nil
}

// Only used from boot, do not use after boot when public methods have their own locks
func (d *db) writeFieldIndexWithLock(fieldName string, dType indexDataType, offset int64, val []uint8, id int) Error {
	d.Lock()

	var p fastjson.Parser

	idx := d.createFieldIndex(fieldName, dType)

	v, err := p.ParseBytes(val)

	if err != nil {
		return newError(SystemMasterErrorCode, UnmarshalFailCode, fmt.Sprintf("Unable to parse document: %s", err.Error()))
	}

	var idxVal interface{}
	if dType == stringIndexType {
		idxVal = v.GetStringBytes(fieldName)
	} else if dType == intIndexType {
		idxVal = v.GetInt(fieldName)
	} else if dType == floatIndexType {
		idxVal = v.GetFloat64(fieldName)
	} else if dType == boolIndexType {
		idxVal = v.GetBool(fieldName)
	}

	idx.Add(offset, idxVal, d.getBlockId(id))

	d.Unlock()

	return nil
}

func (d *db) createFieldIndex(fieldName string, dType indexDataType) *fieldIndex {
	if idx, ok := d.FieldIndex[fieldName]; ok {
		return idx
	}

	d.FieldIndex[fieldName] = newFieldIndex(dType)

	return d.FieldIndex[fieldName]
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
	d.DocCount = make(map[uint16]int)
	d.FieldIndex = make(map[string]*fieldIndex)
	d.FieldIndexKeys = make([]string, 0)
}

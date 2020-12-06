package rose

type fsDriver struct {
	DbDir string
	Handler *fsDb
	HandlerIdx uint16
}

func newFsDriver(dbDir string) *fsDriver {
	return &fsDriver{
		DbDir: dbDir,
	}
}

func (d *fsDriver) Save(data *[]uint8, mapIdx uint16) (int64, int64, Error) {
	if err := d.loadHandler(mapIdx); err != nil {
		return 0, 0, err
	}

	return d.Handler.Write(data)
}

func (d *fsDriver) Read(index int64, mapIdx uint16) (*[]uint8, Error) {
	if err := d.loadHandler(mapIdx); err != nil {
		return nil, err
	}

	return d.Handler.Read(index)
}

func (d *fsDriver) MarkStrategicDeleted(id *[]uint8, del []uint8, mapIdx uint16, offset int64) Error {
	if err := d.loadHandler(mapIdx); err != nil {
		return err
	}

	return d.Handler.StrategicDelete(id, del, offset)
}

func (d *fsDriver) Shutdown() Error {
	if d.Handler != nil {
		if err := d.Handler.SyncAndClose(); err != nil {
			return err
		}
	}

	d.Handler = nil

	return nil
}

func (d *fsDriver) loadHandler(mapIdx uint16) Error {
	if d.Handler != nil && d.HandlerIdx == mapIdx {
		return nil
	}

	if d.Handler != nil {
		if err := d.Handler.SyncAndClose(); err != nil {
			return err
		}
	}

	handler, err := newFsDb(mapIdx, d.DbDir)

	if err != nil {
		return err
	}

	d.Handler = handler
	d.HandlerIdx = mapIdx

	return nil
}

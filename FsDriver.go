package rose

type fsDriver struct {
	DbDir string
	CurrentHandler *fsDb
	CurrentHandlerIdx uint16
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

	return d.CurrentHandler.Write(data)
}

func (d *fsDriver) Read(index int64, mapIdx uint16) (*[]uint8, Error) {
	if err := d.loadHandler(mapIdx); err != nil {
		return nil, err
	}

	return d.CurrentHandler.Read(index)
}

func (d *fsDriver) MarkStrategicDeleted(id *[]uint8, mapIdx uint16, offset int64) Error {
	if err := d.loadHandler(mapIdx); err != nil {
		return err
	}

	return d.CurrentHandler.StrategicDelete(id, offset)
}

func (d *fsDriver) Shutdown() Error {
	if d.CurrentHandler != nil {
		if err := d.CurrentHandler.SyncAndClose(); err != nil {
			return err
		}
	}

	d.CurrentHandler = nil

	return nil
}

func (d *fsDriver) loadHandler(mapIdx uint16) Error {
	if d.CurrentHandler != nil && d.CurrentHandlerIdx == mapIdx {
		return nil
	}

	if d.CurrentHandler != nil {
		if err := d.CurrentHandler.SyncAndClose(); err != nil {
			return err
		}
	}

	handler, err := newFsDb(mapIdx, d.DbDir)

	if err != nil {
		return err
	}

	d.CurrentHandler = handler
	d.CurrentHandlerIdx = mapIdx

	return nil
}

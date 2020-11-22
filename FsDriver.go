package rose

type fsDriver struct {
	Handlers map[uint16]*fsDb
	DbDir string
	CurrentHandler *fsDb
	CurrentHandlerIdx uint16
}

func newFsDriver(dbDir string) *fsDriver {
	return &fsDriver{
		Handlers: make(map[uint16]*fsDb),
		DbDir: dbDir,
	}
}

func (d *fsDriver) Save(data *[]uint8, mapIdx uint16) (int64, int64, Error) {
	if err := d.loadHandler(mapIdx); err != nil {
		return 0, 0, err
	}

	return d.CurrentHandler.Write(data)
}

func (d *fsDriver) MarkDeleted(id *[]uint8, mapIdx uint16) Error {
	if err := d.loadHandler(mapIdx); err != nil {
		return err
	}

	return d.CurrentHandler.Delete(id)
}

func (d *fsDriver) MarkStrategicDeleted(id *[]uint8, mapIdx uint16, offset int64) Error {
	if err := d.loadHandler(mapIdx); err != nil {
		return err
	}

	return d.CurrentHandler.StrategicDelete(id, offset)
}

func (d *fsDriver) Shutdown() Error {
	for _, handler := range d.Handlers {
		if handler.File != nil {
			if err := handler.SyncAndClose(); err != nil {
				return err
			}
		}
	}

	d.Handlers = make(map[uint16]*fsDb)

	return nil
}

func (d *fsDriver) loadHandler(mapIdx uint16) Error {
	if d.CurrentHandler != nil && d.CurrentHandlerIdx == mapIdx {
		return nil
	}

	if d.CurrentHandler != nil {
		if err := d.CurrentHandler.Sleep(); err != nil {
			return err
		}
	}

	handler, ok := d.Handlers[mapIdx]

	if !ok {
		handler, err := newFsDb(mapIdx, d.DbDir)

		if err != nil {
			return err
		}

		d.Handlers[mapIdx] = handler

		d.CurrentHandler = handler
		d.CurrentHandlerIdx = mapIdx

		return nil
	}

	d.CurrentHandler = handler
	d.CurrentHandlerIdx = mapIdx

	return nil
}

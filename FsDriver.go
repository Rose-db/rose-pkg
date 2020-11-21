package rose

type fsDriver struct {
	Handlers map[uint16]*fsDb
	DbDir string
	CurrentHandler *fsDb
	CurrentHandlerIdx uint16
}

type job struct {
	Entry *[]uint8
}

func newFsDriver(dbDir string) *fsDriver {
	return &fsDriver{
		Handlers: make(map[uint16]*fsDb),
		DbDir: dbDir,
	}
}

func (d *fsDriver) Save(j *[]*job, mapIdx uint16) Error {
	if len(*j) == 1 {
		job := (*j)[0]

		if err := d.loadHandler(mapIdx); err != nil {
			return err
		}

		return d.CurrentHandler.Write(job.Entry)
	}

	return nil
}

func (d *fsDriver) MarkDeleted(j *[]*job, mapIdx uint16) Error {
	if len(*j) == 1 {
		if err := d.loadHandler(mapIdx); err != nil {
			return err
		}

		return d.CurrentHandler.Delete((*j)[0].Entry)
	}

	return nil
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

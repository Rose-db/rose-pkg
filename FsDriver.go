package rose

type fsDriver struct {
	Handlers map[uint16]*fsDb
	CurrentHandler *fsDb
	CurrentHandlerIdx uint16
}

type job struct {
	Entry *[]uint8
}

func newFsDriver() *fsDriver {
	return &fsDriver{
		Handlers: make(map[uint16]*fsDb),
	}
}

func (d *fsDriver) Save(j *[]*job, mapIdx uint16) RoseError {
	if len(*j) == 1 {
		job := (*j)[0]

		if err := d.loadHandler(mapIdx); err != nil {
			return err
		}

		return d.CurrentHandler.Write(job.Entry)
	}

	return nil
}

func (d *fsDriver) MarkDeleted(j *[]*job, mapIdx uint16) RoseError {
	if err := d.loadHandler(mapIdx); err != nil {
		return err
	}

	if len(*j) == 1 {
		return d.CurrentHandler.Delete((*j)[0].Entry)
	}

	return nil
}

func (d *fsDriver) Shutdown() RoseError {
	for _, handler := range d.Handlers {
		if handler.File != nil {
			if err := handler.SyncAndClose(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *fsDriver) loadHandler(mapIdx uint16) RoseError {
	handler, ok := d.Handlers[mapIdx]

	if !ok {
		handler, err := newFsDb(mapIdx)

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

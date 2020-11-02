package rose

type fsDriver struct {
	Handlers map[uint16]*fsDb
	CurrentHandler *fsDb
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
		var err RoseError
		job := (*j)[0]
		handler, ok := d.Handlers[mapIdx]

		if !ok {
			handler, err = newFsDb(mapIdx)

			if err != nil {
				return err
			}

			d.Handlers[mapIdx] = handler

			d.CurrentHandler = handler
		}

		return d.CurrentHandler.Write(job.Entry)
	}

	return nil
}

func (d *fsDriver) DeleteSync(j *job) {
}

func (d *fsDriver) Close() RoseError {
	for _, handler := range d.Handlers {
		err := handler.SyncAndClose()

		if err != nil {
			return err
		}
	}

	return nil
}

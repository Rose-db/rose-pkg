package rose

type fsDriver struct {
	FsDbHandler *fsDb
}

type job struct {
	Entry *[]uint8
}

func (d *fsDriver) NewBlock(b int) {

}

func (d *fsDriver) Save(j *[]*job) RoseError {
	if len(*j) == 1 {
		job := (*j)[0]
		return d.FsDbHandler.Write(job.Entry)
	}

	return nil
}

func (d *fsDriver) DeleteSync(j *job) {
	d.FsDbHandler.Delete(j.Entry)
}

func (d *fsDriver) Close() RoseError {
	return d.FsDbHandler.SyncAndClose()
}

func newFsDriver(fsDb *fsDb) *fsDriver {
	return &fsDriver{
		FsDbHandler: fsDb,
	}
}

package rose

type fsDriver struct {
	FsDbHandler *fsDb
}

type job struct {
	Entry *[]uint8
}

func (jb *fsDriver) AddSync(j *job) RoseError {
	return jb.FsDbHandler.Write(j.Entry)
}

func (jb *fsDriver) DeleteSync(j *job) {
	jb.FsDbHandler.Delete(j.Entry)
}

func (jb *fsDriver) Close() RoseError {
	return jb.FsDbHandler.SyncAndClose()
}

func newFsDriver(fsDb *fsDb) *fsDriver {
	return &fsDriver{
		FsDbHandler: fsDb,
	}
}

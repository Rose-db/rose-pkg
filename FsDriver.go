package rose

import (
	"os"
)

type fsDriver struct {
	DbDir string
	DriverType driverType
	Handler *fsDb
	HandlerIdx uint16
}

func newFsDriver(dbDir string, t driverType) (*fsDriver, Error) {
	db := &fsDriver{
		DbDir: dbDir,
		DriverType: t,
	}

	if err := db.loadHandler(0); err != nil {
		return nil, err
	}

	return db, nil
}

func (d *fsDriver) Save(data []uint8, mapIdx uint16) (int64, int64, Error) {
	if d.DriverType != writeDriver {
		return 0, 0, newError(SystemMasterErrorCode, AppInvalidUsageCode, "Driver not used correctly. This driver must be used as a write driver only")
	}

	if err := d.loadHandler(mapIdx); err != nil {
		return 0, 0, err
	}

	return d.Handler.Write(data)
}

func (d *fsDriver) ReadStrategic(index int64, mapIdx uint16) (*lineReaderData, Error) {
	if err := d.loadHandler(mapIdx); err != nil {
		return nil, err
	}

	return d.Handler.ReadStrategic(index)
}

func (d *fsDriver) MarkStrategicDeleted(id []uint8, del []uint8, mapIdx uint16, offset int64) Error {
	if d.DriverType != updateDriver {
		return newError(SystemMasterErrorCode, AppInvalidUsageCode, "Driver not used correctly. This driver must be used as an update driver only")
	}

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

	t := 0
	if d.DriverType == writeDriver {
		t = os.O_RDWR|os.O_CREATE|os.O_APPEND
	} else if d.DriverType == updateDriver {
		t = os.O_RDWR|os.O_CREATE
	}

	handler, err := newFsDb(mapIdx, d.DbDir, t)

	if err != nil {
		return err
	}

	d.Handler = handler
	d.HandlerIdx = mapIdx

	return nil
}

func (d *fsDriver) reload() Error {
	if err := d.Shutdown(); err != nil {
		return err
	}

	if err := d.loadHandler(d.HandlerIdx); err != nil {
		return err
	}

	return nil
}
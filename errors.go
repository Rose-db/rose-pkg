package rose

import (
	"encoding/json"
)

type masterCode int
type code int

type Error interface {
	Error() string
	GetCode() int
	GetMasterCode() int
	JSON() []uint8
}

type dbError struct {
	MasterCode int
	Code int
	Message string
}

func (e *dbError) Error() string {
	return e.Message
}

func (e *dbError) GetCode() int {
	return e.Code
}

func (e *dbError) GetMasterCode() int {
	return e.MasterCode
}

func (e *dbError) JSON() []uint8 {
	return errToJson(e)
}

func newError(m masterCode, c code, msg string) Error {
	return &dbError{
		MasterCode: int(m),
		Code: int(c),
		Message: msg,
	}
}

func errToJson(e Error) []uint8 {
	j := map[string]interface{}{
		"masterCode": e.GetMasterCode(),
		"code": e.GetCode(),
		"message": e.Error(),
	}

	b, _ := json.Marshal(j)

	return b
}

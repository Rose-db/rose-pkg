package rose

import (
	"fmt"
)

type RoseError interface {
	Error() string
	Type() string
	GetCode() int
	JSON() map[string]interface{}
}

type systemError struct {
	Code int
	Message string
}

type httpError struct {
	Code int
	Message string
}

type dbIntegrityError struct {
	Code int
	Message string
}



func (e *systemError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func (e *systemError) Type() string {
	return systemErrorType
}

func (e *systemError) GetCode() int {
	return SystemErrorCode
}

func (e *systemError) JSON() map[string]interface{} {
	return map[string]interface{}{}
}



func (e *dbIntegrityError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func (e *dbIntegrityError) Type() string {
	return systemErrorType
}

func (e *dbIntegrityError) GetCode() int {
	return SystemErrorCode
}

func (e *dbIntegrityError) JSON() map[string]interface{} {
	return map[string]interface{}{}
}




func (e *httpError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func (e *httpError) Type() string {
	return httpErrorType
}

func (e *httpError) GetCode() int {
	return HttpErrorCode
}

func (e *httpError) JSON() map[string]interface{} {
	return map[string]interface{}{}
}

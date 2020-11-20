package rose

import (
	"fmt"
)

type Error interface {
	Error() string
	Type() string
	GetCode() int
	JSON() map[string]interface{}
}

type systemError struct {
	Code int
	Message string
}

type dataError struct {
	Code int
	Message string
}

type dbIntegrityError struct {
	Code int
	Message string
}

type dbError struct {
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


func (e *dbError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func (e *dbError) Type() string {
	return dbErrorType
}

func (e *dbError) GetCode() int {
	return DbErrorCode
}

func (e *dbError) JSON() map[string]interface{} {
	return map[string]interface{}{}
}



func (e *dbIntegrityError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func (e *dbIntegrityError) Type() string {
	return dbIntegrityErrorType
}

func (e *dbIntegrityError) GetCode() int {
	return SystemErrorCode
}

func (e *dbIntegrityError) JSON() map[string]interface{} {
	return map[string]interface{}{}
}



func (e *dataError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func (e *dataError) Type() string {
	return metadataErrorType
}

func (e *dataError) GetCode() int {
	return DataErrorCode
}

func (e *dataError) JSON() map[string]interface{} {
	return map[string]interface{}{}
}

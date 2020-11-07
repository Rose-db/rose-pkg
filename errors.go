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

type metadataError struct {
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




func (e *metadataError) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}

func (e *metadataError) Type() string {
	return metadataErrorType
}

func (e *metadataError) GetCode() int {
	return MetadataErrorCode
}

func (e *metadataError) JSON() map[string]interface{} {
	return map[string]interface{}{}
}

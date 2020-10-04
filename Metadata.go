package rose

import (
	"fmt"
)

type Metadata struct {
	Method 	string
	Id 		string
	Data 	*[]byte
}

func (m *Metadata) validate() RoseError {
	var v []string = []string{"insert", "read", "delete"}

	if !utilHasString(m.Method, v) {
		return &httpError{
			Code:    HttpErrorCode,
			Message: fmt.Sprintf("Method %s does not exist", m.Method),
		}
	}

	if m.Id == "" {
		return &httpError{
			Code:    HttpErrorCode,
			Message: fmt.Sprintf("Id cannot be an empty string"),
		}
	}

	return nil
}



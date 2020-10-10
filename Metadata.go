package rose

import (
	"fmt"
)

type Metadata struct {
	Id 		string
	Data 	[]byte
}

func (m *Metadata) validate() RoseError {
	if m.Id == "" {
		return &httpError{
			Code:    HttpErrorCode,
			Message: fmt.Sprintf("Id cannot be an empty string"),
		}
	}

	return nil
}



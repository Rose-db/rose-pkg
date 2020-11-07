package rose

import (
	"fmt"
)

type Metadata struct {
	Id 		string
	Data 	[]uint8
}

func (m *Metadata) validate() RoseError {
	if m.Id == "" {
		return &metadataError{
			Code:    MetadataErrorCode,
			Message: fmt.Sprintf("Id cannot be an empty string"),
		}
	}

	l := len(m.Id)
	if l > maxIdSize {
		return &metadataError{
			Code:    MetadataErrorCode,
			Message: fmt.Sprintf("Id cannot be larger than 128 bytes, %d bytes given", l),
		}
	}

	if !isJSON(m.Data) {
		return &metadataError{
			Code:    MetadataErrorCode,
			Message: "Data must be a JSON byte array",
		}
	}

	l = len(m.Data)
	if len(m.Data) > maxValSize {
		return &metadataError{
			Code:    MetadataErrorCode,
			Message: fmt.Sprintf("Data cannot be larger than 16000000 bytes (16MB), %d bytes given", l),
		}
	}

	return nil
}



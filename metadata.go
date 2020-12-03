package rose

type Validator interface {
	Validate() Error
}

type WriteMetadata struct {
	CollectionName string
	Data []uint8
}

type ReadMetadata struct {
	CollectionName string
	ID int
	Data interface{}
}

type DeleteMetadata struct {
	CollectionName string
	ID int
}

func (m WriteMetadata) Validate() Error {
	if m.CollectionName == "" {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid collection name. Collection name cannot be an empty string",
		}
	}

	if len(m.Data) == 0 {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid write method data. Data cannot be empty",
		}
	}

	return nil
}

func (m ReadMetadata) Validate() Error {
	if m.CollectionName == "" {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid collection name. Collection name cannot be an empty string",
		}
	}

	return nil
}

func (m DeleteMetadata) Validate() Error {
	if m.CollectionName == "" {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid collection name. Collection name cannot be an empty string",
		}
	}

	return nil
}

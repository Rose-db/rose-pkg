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

type ReplaceMetadata struct {
	CollectionName string
	ID int
	Data []uint8
}

func (m WriteMetadata) Validate() Error {
	if m.CollectionName == "" {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid collection name. Collection name cannot be an empty string",
		}
	}

	if m.Data == nil {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid write method data. Data is empty. Data must be a non empty byte array",
		}
	}

	if len(m.Data) == 0 {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid write method data. Data is empty. Data must be a non empty byte array",
		}
	}

	return nil
}

func (m ReplaceMetadata) Validate() Error {
	if m.CollectionName == "" {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid collection name. Collection name cannot be an empty string",
		}
	}

	if m.Data == nil {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid replace method data. Data is empty. Data must be a non empty byte array",
		}
	}

	if len(m.Data) == 0 {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid replace method data. Data is empty. Data must be a non empty byte array",
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

	if m.Data == nil {
		return &validationError{
			Code:    ValidationErrorCode,
			Message: "Validation error. Invalid read method data. Data is empty. Data must be a non empty byte array",
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

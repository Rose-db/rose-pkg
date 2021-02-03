package rose

type Validator interface {
	Validate() Error
}

type WriteMetadata struct {
	CollectionName string `json:"collectionName"`
	Data interface{} `json:"data"`
}

type ReadMetadata struct {
	CollectionName string `json:"collectionName"`
	ID int `json:"id"`
	Data interface{} `json:"data"`
}

type DeleteMetadata struct {
	CollectionName string
	ID int
}

type ReplaceMetadata struct {
	CollectionName string
	ID int
	Data interface{}
}

func (m WriteMetadata) Validate() Error {
	if m.CollectionName == "" {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid collection name. Collection name cannot be an empty string")
	}

	if m.Data == nil {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid write method data. Data is empty. Data must be a non empty byte array")
	}

	d := m.Data.(string)

	if len(d) == 0 {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid write method data. Data is empty. Data must be a non empty byte array")
	}

	if !isJSON([]uint8(d)) {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Data must be a JSON byte array")
	}

	return nil
}

func (m ReplaceMetadata) Validate() Error {
	if m.CollectionName == "" {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid collection name. Collection name cannot be an empty string")
	}

	if m.Data == nil {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid replace method data. Data is empty. Data must be a non empty byte array")
	}

	d := m.Data.(string)

	if len(d) == 0 {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid replace method data. Data is empty. Data must be a non empty byte array")
	}

	if !isJSON([]uint8(d)) {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Data must be a JSON byte array")
	}

	return nil
}

func (m ReadMetadata) Validate() Error {
	if m.CollectionName == "" {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid collection name. Collection name cannot be an empty string")
	}

	if m.Data == nil {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid read method data. Data is empty. Data must be a non empty byte array")
	}

	return nil
}

func (m DeleteMetadata) Validate() Error {
	if m.CollectionName == "" {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid collection name. Collection name cannot be an empty string")
	}

	return nil
}

package rose

import "fmt"

type Validator interface {
	Validate() Error
}

type WriteMetadata struct {
	CollectionName string `json:"collectionName"`
	Data interface{} `json:"data"`
}

type Pagination struct {
	Page int
	Limit int
}

type ReadByMetadata struct {
	CollectionName string `json:"collectionName"`
	Field string `json:"field"`
	Value interface{} `json:"value"`
	Data interface{} `json:"data"`
	Pagination Pagination
	DataType indexDataType `json:"dataType"`
	Sort sortType
}

type BulkWriteMetadata struct {
	CollectionName string `json:"collectionName"`
	Data []interface{} `json:"data"`
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

func (m ReadByMetadata) Validate(idxKeys []string) Error {
	if m.Field == "" {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid readBy method. 'field' is empty. 'field' must be a non empty string")
	}

	if m.Sort != "" && m.Sort != sortAsc && m.Sort != sortDesc {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Validation error. Invalid sort options. Sort can be only '%s' or '%s'", sortAsc, sortDesc))
	}

	if !hasString(idxKeys, m.Field) {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, fmt.Sprintf("Validation error. Invalid readBy method. '%s' does not exist as an index. In using readBy, 'field' must be indexed", m.Field))
	}

	if m.Value == nil {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid readBy method 'value'. 'value' is empty. 'value' must be a non nil value that corresponds to 'dataType' (int data type -> value must be int)")
	}

	if !isIndexDataType(string(m.DataType)) {
		return newError(ValidationMasterErrorCode, InvalidUserSuppliedDataCode, "Validation error. Invalid readBy method 'dataType'. 'dataType' is an invalid data type. Valid data types are int, float, string and bool")
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

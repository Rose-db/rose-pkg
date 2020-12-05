package rose

import (
	"github.com/onsi/gomega"
)

var _ = GinkgoDescribe("Metadata tests", func() {
	GinkgoIt("Should validate WriteMetadata", func() {
		m := WriteMetadata{
			CollectionName: "",
			Data:           nil,
		}

		err := m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(ValidationErrorCode))
		gomega.Expect(err.Type()).To(gomega.Equal(validationErrorType))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Validation error. Invalid collection name. Collection name cannot be an empty string"))

		m = WriteMetadata{
			CollectionName: "some_name",
			Data:           nil,
		}

		err = m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(ValidationErrorCode))
		gomega.Expect(err.Type()).To(gomega.Equal(validationErrorType))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Validation error. Invalid write method data. Data is empty. Data must be a non empty byte array"))
	})

	GinkgoIt("Should validate ReadMetadata", func() {
		m := ReadMetadata{
			CollectionName: "",
			Data:           nil,
		}

		err := m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(ValidationErrorCode))
		gomega.Expect(err.Type()).To(gomega.Equal(validationErrorType))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Validation error. Invalid collection name. Collection name cannot be an empty string"))

		m = ReadMetadata{
			CollectionName: "some_name",
			Data:           nil,
		}

		err = m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(ValidationErrorCode))
		gomega.Expect(err.Type()).To(gomega.Equal(validationErrorType))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Validation error. Invalid read method data. Data is empty. Data must be a non empty byte array"))
	})

	GinkgoIt("Should validate DeleteMetadata", func() {
		m := DeleteMetadata{
			CollectionName: "",
		}

		err := m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(ValidationErrorCode))
		gomega.Expect(err.Type()).To(gomega.Equal(validationErrorType))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 6, Message: Validation error. Invalid collection name. Collection name cannot be an empty string"))
	})
})

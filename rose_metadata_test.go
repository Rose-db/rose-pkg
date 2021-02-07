package rose

import (
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = GinkgoDescribe("Metadata tests", func() {
	GinkgoIt("Should validate WriteMetadata", func() {
		ginkgo.Skip("")

		m := WriteMetadata{
			CollectionName: "",
			Data:           nil,
		}

		err := m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid collection name. Collection name cannot be an empty string"))

		m = WriteMetadata{
			CollectionName: "some_name",
			Data:           nil,
		}

		err = m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid write method data. Data is empty. Data must be a non empty byte array"))
	})

	GinkgoIt("Should validate ReadMetadata", func() {
		ginkgo.Skip("")

		m := ReadMetadata{
			CollectionName: "",
			Data:           nil,
		}

		err := m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid collection name. Collection name cannot be an empty string"))

		m = ReadMetadata{
			CollectionName: "some_name",
			Data:           nil,
		}

		err = m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid read method data. Data is empty. Data must be a non empty byte array"))
	})

	GinkgoIt("Should validate DeleteMetadata", func() {
		ginkgo.Skip("")

		m := DeleteMetadata{
			CollectionName: "",
		}

		err := m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid collection name. Collection name cannot be an empty string"))
	})

	GinkgoIt("Should validate ReplaceMetadata", func() {
		ginkgo.Skip("")

		m := ReplaceMetadata{
			CollectionName: "",
		}

		err := m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid collection name. Collection name cannot be an empty string"))

		m = ReplaceMetadata{
			CollectionName: "coll_name",
			ID: 0,
			Data: "",
		}

		err = m.Validate()

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Validation error. Invalid replace method data. Data is empty. Data must be a non empty byte array"))
	})
})


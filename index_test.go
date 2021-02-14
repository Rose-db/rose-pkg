package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"io/ioutil"
	"os"
)

var _ = GinkgoDescribe("Index tests", func() {
	GinkgoIt("Should create index file on rose create", func() {
		a := testCreateRose(false)

		idxLoc := roseIndexLocation()

		_, err := os.Stat(idxLoc)

		if os.IsNotExist(err) {
			ginkgo.Fail(fmt.Sprintf("Failed creating index file on the filesystem: %s", err.Error()))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Assert that index cannot be created on a non existent collection", func() {
		a := testCreateRose(false)

		err := a.NewIndex("not_exists", "some_field", stringIndexType)

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Invalid index request. Collection not_exists does not exist"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Assert that index cannot be created if a field name is an empty string", func() {
		a := testCreateRose(false)

		err := a.NewCollection("coll_name")

		gomega.Expect(err).To(gomega.BeNil())

		err = a.NewIndex("coll_name", "", stringIndexType)

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Index field name cannot be an empty string"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should assert that an index is written to the filesystem", func() {
		a := testCreateRose(false)

		err := a.NewCollection("coll_name")

		gomega.Expect(err).To(gomega.BeNil())

		err = a.NewIndex("coll_name", "field", stringIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		idxLoc := roseIndexLocation()

		b, e := ioutil.ReadFile(idxLoc)

		gomega.Expect(e).To(gomega.BeNil())
		created := fmt.Sprintf("%s%s%s%s%s\n", "coll_name", delim, "field", delim, stringIndexType)

		gomega.Expect(string(b)).To(gomega.Equal(created))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should assert that an index did not create if it is equal to the one created before", func() {
		a := testCreateRose(false)

		err := a.NewCollection("coll_name")

		gomega.Expect(err).To(gomega.BeNil())

		err = a.NewIndex("coll_name", "field", stringIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		idxLoc := roseIndexLocation()

		b, e := ioutil.ReadFile(idxLoc)

		gomega.Expect(e).To(gomega.BeNil())
		created := fmt.Sprintf("%s%s%s%s%s\n", "coll_name", delim, "field", delim, stringIndexType)

		gomega.Expect(string(b)).To(gomega.Equal(created))

		err = a.NewIndex("coll_name", "field", stringIndexType)

		b, e = ioutil.ReadFile(idxLoc)

		gomega.Expect(e).To(gomega.BeNil())
		created = fmt.Sprintf("%s%s%s%s%s\n", "coll_name", delim, "field", delim, stringIndexType)

		gomega.Expect(string(b)).To(gomega.Equal(created))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should assert that an is read from the filesystem in a proper format", func() {
		a := testCreateRose(false)

		colls := make([]string, 0)

		colls = append(colls, testCreateCollection(a, "coll_1"))
		colls = append(colls, testCreateCollection(a, "coll_2"))
		colls = append(colls, testCreateCollection(a, "coll_3"))
		colls = append(colls, testCreateCollection(a, "coll_4"))

		ih, err := newIndexHandler()

		gomega.Expect(err).To(gomega.BeNil())

		for _, coll := range colls {
			fieldName := fmt.Sprintf("%s_field_name", coll)
			err := ih.Add(fsIndex{
				Name:     coll,
				Field:    fieldName,
				DataType: stringIndexType,
			})

			gomega.Expect(err).To(gomega.BeNil())

			idx, err := ih.Find(coll)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(idx).To(gomega.Not(gomega.BeNil()))

			gomega.Expect(idx.Name).To(gomega.Equal(coll))
			gomega.Expect(idx.Field).To(gomega.Equal(fieldName))
			gomega.Expect(idx.DataType).To(gomega.Equal(stringIndexType))
		}

		err = ih.Close()

		gomega.Expect(err).To(gomega.BeNil())

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})


package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"io/ioutil"
)

var _ = GinkgoDescribe("Successfully failing tests", func() {
	GinkgoIt("Should fail if data is not a json byte array", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)

		data := "string_that_is_not_json"

		_, err := a.Write(WriteMetadata{Data: []uint8(data)})

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(DataErrorCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		gomega.Expect(err.Error()).To(gomega.Equal("Code: 1, Message: Data must be a JSON byte array"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail because data too large > 16MB", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)

		str, fsErr := ioutil.ReadFile("large_value.txt")

		if fsErr != nil {
			panic(fsErr)
		}

		// generates a > 16Mb string
		generateData := func() []uint8 {
			s := string(str)

			for {
				s += s

				if len(s) > maxValSize {
					return []uint8(s)
				}
			}
		}

		d := generateData()

		_, err := a.Write(WriteMetadata{Data: testAsJson(string(d))})

		if err == nil {
			ginkgo.Fail("err should not be nil")

			return
		}

		gomega.Expect(err.GetCode()).To(gomega.Equal(DataErrorCode), fmt.Sprintf("DataErrorCode should have been returned as Error.Status"))
		// TODO: There seems to be a difference when converting json byte array to string and back into byte array, check later
		//gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Code: 1, Message: %s", fmt.Sprintf("Data cannot be larger than 16000000 bytes (16MB), %d bytes given", len(d)))))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to read a document if not exists", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)

		var s string
		res, err := a.Read(ReadMetadata{ID: 67, Data: &s})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail to delete a document if not exist", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)

		res, err := a.Delete(DeleteMetadata{ID: 89})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})


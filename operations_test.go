package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = GinkgoDescribe("Insertion tests", func() {
	GinkgoIt("Should insert a single piece of data", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		a := testCreateRose(false)

		res := testSingleConcurrentInsert(WriteMetadata{Data: s}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should insert multiple values", func() {
		var currId uint64

		a := testCreateRose(false)

		for i := 0; i < 100000; i++ {
			s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

			res := testSingleConcurrentInsert(WriteMetadata{Data: s}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			currId++
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

var _ = GinkgoDescribe("Read tests", func() {
	GinkgoIt("Should read a single result", func() {
		a := testCreateRose(false)

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		temp := testSingleConcurrentInsert(WriteMetadata{Data: s}, a)
		id := temp.ID

		r := ""
		res, err := a.Read(ReadMetadata{ID: id, Data: &r})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(r).To(gomega.Equal("sdčkfjalsčkjfdlsčakdfjlčk"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should perform multiple reads", func() {
		a := testCreateRose(false)

		ids := make([]int, 0)
		for i := 0; i < 100000; i++ {
			value := testAsJson(fmt.Sprintf("id-value-%d", i))

			res := testSingleConcurrentInsert(WriteMetadata{Data: value}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids = append(ids, res.ID)
		}

		for _, id := range ids {
			r := ""
			res, err := a.Read(ReadMetadata{ID: id, Data: &r})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert fs db integrity after multiple inserts", func() {
		a := testCreateRose(false)

		ids := make([]int, 0)
		fsData := ""
		for i := 0; i < 10000; i++ {
			value := testAsJson(fmt.Sprintf("id-value-%d", i))
			data := value

			res, err := a.Write(WriteMetadata{Data: value})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			fsData += string(*prepareData(res.ID, data))

			ids = append(ids, res.ID)
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete a single document", func() {
		a := testCreateRose(false)

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		res := testSingleConcurrentInsert(WriteMetadata{Data: s}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		id := res.ID

		res = testSingleDelete(DeleteMetadata{ID: id}, a)

		gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		r := ""
		res, err := a.Read(ReadMetadata{ID: id, Data: &r})

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err = a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

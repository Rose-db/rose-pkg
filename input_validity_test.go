package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = GinkgoDescribe("Input validity tests", func() {
	GinkgoIt("Should successfully save and read data that is similar to the delimiter", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		data := "[#]{{}#]"

		res := testSingleConcurrentInsert(WriteMetadata{Data: testAsJson(data), CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		key := res.ID
		s := ""
		res = testSingleRead(ReadMetadata{ID: key, Data: &s, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(s).To(gomega.Equal(data))

		res = testSingleDelete(DeleteMetadata{ID: key, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		res = testSingleRead(ReadMetadata{ID: key, Data: &s, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should successfully skip newlines in data values and not treat them as document delimiters", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")

		data := "[#]{{\n}#]\n"

		res := testSingleConcurrentInsert(WriteMetadata{Data: testAsJson(data), CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		key := res.ID
		s := ""
		res = testSingleRead(ReadMetadata{ID: key, Data: &s, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(s).To(gomega.Equal(data))

		res = testSingleDelete(DeleteMetadata{ID: key, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		res = testSingleRead(ReadMetadata{ID: key, Data: &s, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

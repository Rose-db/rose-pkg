package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"strings"
)

var _ = GinkgoDescribe("Insertion tests", func() {
	GinkgoIt("Should insert a single piece of data", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should insert a single piece of data in multiple collection", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		a := testCreateRose(false)

		collOne := testCreateCollection(a, "test_coll_one")
		collTwo := testCreateCollection(a, "test_coll_two")
		collThree := testCreateCollection(a, "test_coll_three")

		res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collOne}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		res = testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collTwo}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		res = testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collThree}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should insert multiple values", func() {
		var currId uint64

		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		for i := 0; i < 100000; i++ {
			s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			currId++
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

var _ = GinkgoDescribe("Read tests", func() {
	GinkgoIt("Should read a single result", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		temp := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)
		id := temp.ID

		r := ""
		res := testSingleRead(ReadMetadata{ID: id, Data: &r, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		gomega.Expect(r).To(gomega.Equal("sdčkfjalsčkjfdlsčakdfjlčk"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should perform multiple reads", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		ids := make([]int, 0)
		for i := 0; i < 100000; i++ {
			value := testAsJson(fmt.Sprintf("id-value-%d", i))

			res := testSingleConcurrentInsert(WriteMetadata{Data: value, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids = append(ids, res.ID)
		}

		for _, id := range ids {
			r := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &r, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should delete a single document", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")

		res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		id := res.ID

		res = testSingleDelete(DeleteMetadata{ID: id, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

		r := ""
		res = testSingleRead(ReadMetadata{ID: id, Data: &r, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should replace a single document", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		s := testAsJson("value_one")

		res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

		id := res.ID

		res = testSingleReplace(ReplaceMetadata{ID: id, CollectionName: collName, Data: testAsJson("value_replaced")}, a)

		gomega.Expect(res.Status).To(gomega.Equal(ReplacedResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReplaceMethodType))

		d := ""
		res = testSingleRead(ReadMetadata{CollectionName: collName, ID: id, Data: &d}, a)

		gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should return 0 written results if user provides 0 documents to write", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		ms := []interface{}{}

		resChan := make(chan *BulkAppResult)
		go func() {
			res, err := a.BulkWrite(BulkWriteMetadata{CollectionName: collName, Data: ms})

			gomega.Expect(err).To(gomega.BeNil())

			resChan<- res
		}()
		res := <-resChan

		gomega.Expect(res.WrittenIDs).To(gomega.Equal(""))
		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(BulkWriteMethodType))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should insert 100 thousand in bulk", func() {
		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		s := testAsJson("\n\nLorem ipsum dolor sit amet, consectetur adipiscing elit. Sed elementum felis vel aliquam scelerisque. Nullam nibh mi, lacinia in euismod vel, ultricies non nisl. Etiam dictum nec ipsum id sodales. Suspendisse eget dictum neque. Etiam ullamcorper orci sed tristique tempor. Proin quis elit commodo enim pretium imperdiet semper vel augue. Donec eu vehicula eros. Proin faucibus sed quam ut tempor. Aenean in facilisis sem. Nullam semper, massa sed ultricies sagittis, tellus lorem tincidunt justo, non laoreet lacus urna at libero.\n\nQuisque id ipsum nec quam mattis rutrum. Mauris sit amet pharetra metus. Aliquam nec sem nec urna pharetra posuere et ac lacus. Ut ligula purus, porta vel pretium vitae, blandit ac nunc. Donec sem turpis, pellentesque in condimentum ac, fermentum in mi. Phasellus commodo faucibus gravida. Curabitur at orci sit amet elit eleifend laoreet quis eget magna. Aliquam pretium tempus neque. Quisque urna purus, vestibulum sit amet sapien id, viverra lacinia nisi. Nullam augue dolor, consectetur ut. ")

		ms := []interface{}{}

		for i := 0; i < 100000; i++ {
			ms = append(ms, s)
		}

		resChan := make(chan *BulkAppResult)
		go func() {
			res, err := a.BulkWrite(BulkWriteMetadata{CollectionName: collName, Data: ms})

			gomega.Expect(err).To(gomega.BeNil())

			resChan<- res
		}()
		res := <-resChan

		gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
		gomega.Expect(res.Method).To(gomega.Equal(BulkWriteMethodType))
		gomega.Expect(len(strings.Split(res.WrittenIDs, ","))).To(gomega.Equal(100000))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

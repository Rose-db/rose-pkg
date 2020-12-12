package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"sync"
	"time"
)

var _ = GinkgoDescribe("Concurrency tests", func() {
	GinkgoIt("Should write values to the database with the concurrent method", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		n := 10000

		results := make(chan *AppResult, n)
		wg := &sync.WaitGroup{}
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int, wg *sync.WaitGroup) {
				s := testAsJson(testString)

				res, err := a.Write(WriteMetadata{Data: s})

				gomega.Expect(err).To(gomega.BeNil())

				results<- res

				wg.Done()
			}(i, wg)
		}

		wg.Wait()

		close(results)

		ids := [10000]int{}
		count := 0
		for res := range results {
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[count] = res.ID

			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		// assert that every uuid is a valid uuid
		count = 0
		for _, id := range ids {
			id = id
			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		// assert that there are no duplicate ids
		uuidsMap := make(map[int]interface{})
		for _, id := range ids {
			_, ok := uuidsMap[id]

			gomega.Expect(ok).To(gomega.BeFalse())

			uuidsMap[id] = nil
		}

		for _, id := range ids {
			s := ""
			res, err := a.Read(ReadMetadata{ID: id, Data: &s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should delete document from the database with write done synchronously", func() {
		ginkgo.Skip("")
		a := testCreateRose(false)
		n := 10000

		ids := [10000]int{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			res, err := a.Write(WriteMetadata{Data: s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		results := [10000]*AppResult{}
		for i, id := range ids {
			res := testSingleDelete(DeleteMetadata{ID: id}, a)
			results[i] = res
		}

		count := 0
		for _, res := range results {
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

			count++
		}

		gomega.Expect(count).To(gomega.Equal(n))

		for _, id := range ids {
			s := ""
			res, err := a.Read(ReadMetadata{ID: id, Data: &s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range ids {
			s := ""
			res, err := a.Read(ReadMetadata{ID: id, Data: &s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should write/delete with sender/receiver patter", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		n := 10000

		ids := [10000]int{}
		results := [10000]*AppResult{}
		for i := 0; i < n; i++ {

			s := testAsJson(testString)

			res := testSingleConcurrentInsert(WriteMetadata{Data: s}, a)

			results[i] = res
		}

		for i, res := range results {
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			delRes := testSingleDelete(DeleteMetadata{ID: res.ID}, a)

			gomega.Expect(delRes.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(delRes.Method).To(gomega.Equal(DeleteMethodType))

			ids[i] = res.ID
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range ids {
			s := ""
			res, err := a.Read(ReadMetadata{ID: id, Data: &s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should write data without waiting for a goroutine to finish and read the results after a timeout", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		n := 10000

		results := [10000]*AppResult{}
		values := [10000]string{}
		for i := 0; i < n; i++ {
			go func(i int) {
				ginkgo.GinkgoRecover()

				value := fmt.Sprintf("value-%d", i)
				s := testAsJson(value)

				res, err := a.Write(WriteMetadata{Data: s})

				gomega.Expect(err).To(gomega.BeNil())

				results[i] = res
				values[i] = value
			}(i)
		}

		time.Sleep(5 * time.Second)

		for i, res := range results {
			value := values[i]

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			s := ""
			appResult, err := a.Read(ReadMetadata{ID: res.ID, Data: &s})

			gomega.Expect(value).To(gomega.Equal(s))

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(appResult.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(appResult.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should delete data without waiting for a goroutine to finish and read the results after a timeout", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		n := 10000

		results := [10000]*AppResult{}
		for i := 0; i < n; i++ {
			value := fmt.Sprintf("value-%d", i)
			s := testAsJson(value)

			res := testSingleConcurrentInsert(WriteMetadata{Data: s}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			results[i] = res
		}

		delResults := [10000]*AppResult{}
		for i, res := range results {
			go func(i int, wRes *AppResult) {
				ginkgo.GinkgoRecover()

				delRes, err := a.Delete(DeleteMetadata{ID: wRes.ID})

				gomega.Expect(err).To(gomega.BeNil())

				delResults[i] = delRes
			}(i, res)
		}

		time.Sleep(5 * time.Second)

		for _, res := range delResults {
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

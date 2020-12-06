package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"time"
)

var _ = GinkgoDescribe("Concurrency tests", func() {
	GinkgoIt("Should write values to the database with the concurrent method", func() {
		a := testCreateRose(false)
		n := 10000

		results := [10000]chan *GoAppResult{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			resChan := a.GoWrite(WriteMetadata{Data: s})

			results[i] = resChan
		}

		ids := [10000]int{}
		count := 0
		for i, c := range results {
			res := <-c

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.Result.ID

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
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete document from the database with write done synchronously", func() {
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

		goResults := [10000]chan *GoAppResult{}
		for i, id := range ids {
			resChan := a.GoDelete(DeleteMetadata{ID: id})

			goResults[i] = resChan
		}

		count := 0
		for _, c := range goResults {
			res := <-c

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(res.Result.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(DeleteMethodType))

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
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should write/delete with sender/receiver patter", func() {
		a := testCreateRose(false)
		n := 10000

		ids := [10000]int{}
		goResults := [10000]chan *GoAppResult{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			resChan := a.GoWrite(WriteMetadata{Data: s})

			goResults[i] = resChan
		}

		for i, resChan := range goResults {
			res := <-resChan

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(WriteMethodType))

			ch := a.GoDelete(DeleteMetadata{ID: res.Result.ID})

			delRes := <-ch

			gomega.Expect(delRes.Err).To(gomega.BeNil())
			gomega.Expect(delRes.Result).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(delRes.Result.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(delRes.Result.Method).To(gomega.Equal(DeleteMethodType))

			ids[i] = res.Result.ID
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
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should write data without waiting for a goroutine to finish and read the results after a timeout", func() {
		a := testCreateRose(false)
		n := 10000

		goResults := [10000]chan *GoAppResult{}
		values := [10000]string{}
		for i := 0; i < n; i++ {
			value := fmt.Sprintf("value-%d", i)
			s := testAsJson(value)

			resChan := a.GoWrite(WriteMetadata{Data: s})

			goResults[i] = resChan
			values[i] = value
		}

		time.Sleep(20 * time.Second)

		for i, resChan := range goResults {
			res := <-resChan
			value := values[i]

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(WriteMethodType))

			s := ""
			appResult, err := a.Read(ReadMetadata{ID: res.Result.ID, Data: &s})

			gomega.Expect(value).To(gomega.Equal(s))

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(appResult.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(appResult.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should delete data without waiting for a goroutine to finish and read the results after a timeout", func() {
		a := testCreateRose(false)
		n := 10000

		results := [10000] *AppResult{}
		for i := 0; i < n; i++ {
			value := fmt.Sprintf("value-%d", i)
			s := testAsJson(value)

			result, err := a.Write(WriteMetadata{Data: s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(result.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(result.Method).To(gomega.Equal(WriteMethodType))

			results[i] = result
		}

		goResults := [10000]chan *GoAppResult{}
		for i, res := range results {
			ch := a.GoDelete(DeleteMetadata{ID: res.ID})

			goResults[i] = ch
		}

		time.Sleep(15 * time.Second)

		for _, resChan := range goResults {
			res := <-resChan

			gomega.Expect(res.Err).To(gomega.BeNil())
			gomega.Expect(res.Result).To(gomega.Not(gomega.BeNil()))
			gomega.Expect(res.Result.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Result.Method).To(gomega.Equal(DeleteMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

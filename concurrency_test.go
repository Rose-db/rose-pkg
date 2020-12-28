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
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll_name")
		n := 10000

		results := make(chan *AppResult, n)
		wg := &sync.WaitGroup{}
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int, wg *sync.WaitGroup) {
				s := testAsJson(testString)

				res, err := a.Write(WriteMetadata{Data: s, CollectionName: collName})

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
			res := testSingleRead(ReadMetadata{ID: id, Data: &s, CollectionName: collName}, a)

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
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")
		n := 10000

		ids := [10000]int{}
		for i := 0; i < n; i++ {
			s := testAsJson(testString)

			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		results := [10000]*AppResult{}
		for i, id := range ids {
			res := testSingleDelete(DeleteMetadata{ID: id, CollectionName: collName}, a)
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
			res := testSingleRead(ReadMetadata{ID: id, Data: &s, CollectionName: collName}, a)

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
			res, err := a.Read(ReadMetadata{ID: id, Data: &s, CollectionName: collName})

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
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")
		n := 10000

		ids := [10000]int{}
		results := [10000]*AppResult{}
		for i := 0; i < n; i++ {

			s := testAsJson(testString)

			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			results[i] = res
		}

		for i, res := range results {
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			delRes := testSingleDelete(DeleteMetadata{ID: res.ID, CollectionName: collName}, a)

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
			res := testSingleRead(ReadMetadata{ID: id, Data: &s, CollectionName: collName}, a)

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
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll_name")
		n := 10000

		results := [10000]*AppResult{}
		values := [10000]string{}
		for i := 0; i < n; i++ {
			go func(i int) {
				ginkgo.GinkgoRecover()

				value := fmt.Sprintf("value-%d", i)
				s := testAsJson(value)

				res, err := a.Write(WriteMetadata{Data: s, CollectionName: collName})

				gomega.Expect(err).To(gomega.BeNil())

				results[i] = res
				values[i] = value
			}(i)
		}

		time.Sleep(2 * time.Second)

		for i, res := range results {
			value := values[i]

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			s := ""
			appResult := testSingleRead(ReadMetadata{ID: res.ID, Data: &s, CollectionName: collName}, a)

			gomega.Expect(value).To(gomega.Equal(s))

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
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")
		n := 10000

		results := [10000]*AppResult{}
		for i := 0; i < n; i++ {
			value := fmt.Sprintf("value-%d", i)
			s := testAsJson(value)

			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			results[i] = res
		}

		delResults := [10000]*AppResult{}
		for i, res := range results {
			go func(i int, wRes *AppResult) {
				ginkgo.GinkgoRecover()

				delRes, err := a.Delete(DeleteMetadata{ID: wRes.ID, CollectionName: collName})

				gomega.Expect(err).To(gomega.BeNil())

				delResults[i] = delRes
			}(i, res)
		}

		time.Sleep(2 * time.Second)

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

	GinkgoIt("Should read all values in spawned goroutines", func() {
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")
		n := 10000

		results := [10000]int{}
		for i := 0; i < n; i++ {
			value := fmt.Sprintf("value-%d", i)
			s := testAsJson(value)

			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			results[i] = res.ID
		}

		appResults := [10000]*AppResult{}
		for i := 0; i < n; i++ {
			go func(i int) {
				s := ""
				res, err := a.Read(ReadMetadata{ID: i + 1, CollectionName: collName, Data: &s})

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(s).To(gomega.Equal(fmt.Sprintf("value-%d", i)))

				appResults[i] = res
			}(i)
		}

		time.Sleep(2 * time.Second)

		for _, res := range appResults {
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

	GinkgoIt("Should delete all values in spawned goroutines", func() {
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")
		n := 10000

		results := [10000]int{}
		for i := 0; i < n; i++ {
			value := fmt.Sprintf("value-%d", i)
			s := testAsJson(value)

			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			results[i] = res.ID
		}

		appResults := [10000]*AppResult{}
		for i := 0; i < n; i++ {
			go func(i int) {
				res, err := a.Delete(DeleteMetadata{ID: i + 1, CollectionName: collName})

				gomega.Expect(err).To(gomega.BeNil())

				appResults[i] = res
			}(i)
		}

		time.Sleep(2 * time.Second)

		for _, res := range appResults {
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

	GinkgoIt("Should read and delete all documents spawned in multiple collections", func() {
		a := testCreateRose(false)
		collOne := testCreateCollection(a, "coll_one")
		collTwo := testCreateCollection(a, "coll_two")
		collThree := testCreateCollection(a, "coll_three")
		n := 10000

		oneIds := [10000]int{}
		twoIds := [10000]int{}
		threeIds := [10000]int{}
		insert := func(ids *[10000]int, collName string) {
			for i := 0; i < n; i++ {
				go func(ids *[10000]int, collName string, i int) {
					defer ginkgo.GinkgoRecover()
					value := fmt.Sprintf("value-%d", i)
					s := testAsJson(value)

					res, err := a.Write(WriteMetadata{Data: s, CollectionName: collName})

					gomega.Expect(err).To(gomega.BeNil())

					ids[i] = res.ID
				}(ids, collName, i)
			}
		}

		insert(&oneIds, collOne)
		insert(&twoIds, collTwo)
		insert(&threeIds, collThree)

		time.Sleep(2 * time.Second)

		readOneResults := [10000]*AppResult{}
		for i, id := range oneIds {
			go func(i int, id int) {
				defer ginkgo.GinkgoRecover()

				s := ""
				res, err := a.Read(ReadMetadata{ID: id, CollectionName: collOne, Data: &s})

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(s).To(gomega.Equal(fmt.Sprintf("value-%d", i)))

				readOneResults[i] = res
			}(i, id)
		}

		readTwoResults := [10000]*AppResult{}
		for i, id := range twoIds {
			go func(i int, id int) {
				defer ginkgo.GinkgoRecover()

				s := ""
				res, err := a.Read(ReadMetadata{ID: id, CollectionName: collTwo, Data: &s})

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(s).To(gomega.Equal(fmt.Sprintf("value-%d", i)))

				readTwoResults[i] = res
			}(i, id)
		}

		readThreeResults := [10000]*AppResult{}
		for i, id := range threeIds {
			go func(i int, id int) {
				defer ginkgo.GinkgoRecover()

				s := ""
				res, err := a.Read(ReadMetadata{ID: id, CollectionName: collThree, Data: &s})

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(s).To(gomega.Equal(fmt.Sprintf("value-%d", i)))

				readThreeResults[i] = res
			}(i, id)
		}

		time.Sleep(5 * time.Second)

		for _, res := range readOneResults {
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		for _, res := range readTwoResults {
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		for _, res := range readThreeResults {
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		deleteOneResults := [10000]*AppResult{}
		for i, id := range oneIds {
			go func(i int, id int) {
				defer ginkgo.GinkgoRecover()

				res, err := a.Delete(DeleteMetadata{ID: id, CollectionName: collOne})

				gomega.Expect(err).To(gomega.BeNil())

				deleteOneResults[i] = res
			}(i, id)
		}

		deleteTwoResults := [10000]*AppResult{}
		for i, id := range twoIds {
			go func(i int, id int) {
				defer ginkgo.GinkgoRecover()

				res, err := a.Delete(DeleteMetadata{ID: id, CollectionName: collTwo})

				gomega.Expect(err).To(gomega.BeNil())

				deleteTwoResults[i] = res
			}(i, id)
		}

		deleteThreeResults := [10000]*AppResult{}
		for i, id := range threeIds {
			go func(i int, id int) {
				defer ginkgo.GinkgoRecover()

				res, err := a.Delete(DeleteMetadata{ID: id, CollectionName: collThree})

				gomega.Expect(err).To(gomega.BeNil())

				deleteThreeResults[i] = res
			}(i, id)
		}

		time.Sleep(5 * time.Second)

		for _, res := range deleteOneResults {
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		for _, res := range deleteTwoResults {
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		for _, res := range deleteThreeResults {
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

	GinkgoIt("Should replace documents concurrently", func() {
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll_one")
		n := 12321

		oneIds := [12321]int{}
		insert := func(ids *[12321]int, collName string) {
			for i := 0; i < n; i++ {
				go func(ids *[12321]int, collName string, i int) {
					defer ginkgo.GinkgoRecover()

					value := fmt.Sprintf("value-%d", i)
					s := testAsJson(value)

					res, err := a.Write(WriteMetadata{Data: s, CollectionName: collName})

					gomega.Expect(err).To(gomega.BeNil())

					ids[i] = res.ID
				}(ids, collName, i)
			}
		}

		updated := [12321]int{}
		replace := func(updated *[12321]int, collName string) {
			for i := 0; i < 12321; i++ {
				go func(updated *[12321]int, collName string, i int) {
					defer ginkgo.GinkgoRecover()

					value := "value-updated"
					s := testAsJson(value)

					res, err := a.Replace(ReplaceMetadata{Data: s, CollectionName: collName, ID: i + 1})

					gomega.Expect(err).To(gomega.BeNil())
					gomega.Expect(res.Method).To(gomega.Equal(ReplaceMethodType))
					gomega.Expect(res.Status).To(gomega.Equal(ReplacedResultStatus))

					updated[i] = res.ID
				}(updated, collName, i)
			}
		}

		insert(&oneIds, collName)

		time.Sleep(2 * time.Second)

		replace(&updated, collName)

		time.Sleep(5 * time.Second)

		for i := 0; i < 12321; i++ {
			s := ""
			res := testSingleRead(ReadMetadata{
				CollectionName: collName,
				ID:             i + 1,
				Data:           &s,
			}, a)

			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
			gomega.Expect(s).To(gomega.Equal("value-updated"))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should replace and delete documents concurrently", func() {
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll_one")
		n := 12321

		oneIds := [12321]int{}
		insert := func(ids *[12321]int, collName string) {
			for i := 0; i < n; i++ {
				go func(ids *[12321]int, collName string, i int) {
					defer ginkgo.GinkgoRecover()

					value := fmt.Sprintf("value-%d", i)
					s := testAsJson(value)

					res, err := a.Write(WriteMetadata{Data: s, CollectionName: collName})

					gomega.Expect(err).To(gomega.BeNil())

					ids[i] = res.ID
				}(ids, collName, i)
			}
		}

		updated := [12321]int{}
		replace := func(updated *[12321]int, collName string) {
			for i := 0; i < 12321; i++ {
				go func(updated *[12321]int, collName string, i int) {
					defer ginkgo.GinkgoRecover()

					value := "value-updated"
					s := testAsJson(value)

					res, err := a.Replace(ReplaceMetadata{Data: s, CollectionName: collName, ID: i + 1})

					gomega.Expect(err).To(gomega.BeNil())
					gomega.Expect(res.Method).To(gomega.Equal(ReplaceMethodType))
					gomega.Expect(res.Status).To(gomega.Equal(ReplacedResultStatus))

					updated[i] = res.ID
				}(updated, collName, i)
			}
		}

		deleted := [12321]int{}
		del := func(deleted *[12321]int, collName string) {
			for i := 0; i < 12321; i++ {
				go func(deleted *[12321]int, collName string, i int) {
					defer ginkgo.GinkgoRecover()

					res, err := a.Delete(DeleteMetadata{CollectionName: collName, ID: i + 1})

					gomega.Expect(err).To(gomega.BeNil())
					gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
					gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))

					deleted[i] = res.ID
				}(deleted, collName, i)
			}
		}

		insert(&oneIds, collName)

		time.Sleep(2 * time.Second)

		replace(&updated, collName)
		del(&deleted, collName)

		time.Sleep(5 * time.Second)

		for i := 0; i < 12321; i++ {
			s := ""
			res := testSingleRead(ReadMetadata{
				CollectionName: collName,
				ID:             i + 1,
				Data:           &s,
			}, a)

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

	GinkgoIt("Should concurrently write to multiple collections", func() {
		a := testCreateRose(false)
		collOne := testCreateCollection(a, "coll_one")
		collTwo := testCreateCollection(a, "coll_one")
		collThree := testCreateCollection(a, "coll_one")

		n := 100000

		wg := &sync.WaitGroup{}
		wg.Add(3)
		go func() {
			for i := 0; i < n; i++ {
				someData := "slkdjfasjdfklsajdflsadf"
				res, err := a.Write(WriteMetadata{
					CollectionName: collOne,
					Data:           testAsJson(someData),
				})

				gomega.Expect(err).To(gomega.BeNil())

				gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
			}

			wg.Done()
		}()

		go func() {
			for i := 0; i < n; i++ {
				someData := "slkdjfasjdfklsajdflsadf"
				res, err := a.Write(WriteMetadata{
					CollectionName: collTwo,
					Data:           testAsJson(someData),
				})

				gomega.Expect(err).To(gomega.BeNil())

				gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
			}

			wg.Done()
		}()

		go func() {
			for i := 0; i < n; i++ {
				someData := "slkdjfasjdfklsajdflsadf"
				res, err := a.Write(WriteMetadata{
					CollectionName: collThree,
					Data:           testAsJson(someData),
				})

				gomega.Expect(err).To(gomega.BeNil())

				gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
			}

			wg.Done()
		}()

		wg.Wait()

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

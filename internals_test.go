package rose

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = GinkgoDescribe("Internal Memory DB tests", func() {
	GinkgoIt("Should inspect block tracker in multiple collections", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		collOne := testCreateCollection(a, "coll_one")
		collTwo := testCreateCollection(a, "coll_two")
		collThree := testCreateCollection(a, "coll_three")

		collections := []string{collOne, collTwo, collThree}

		n := 10000

		for _, collName := range collections {
			for i := 0; i < n; i++ {
				res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

				gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
			}

			dirs, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collName))

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(len(dirs)).To(gomega.Equal(n/blockMark + 1))

			db := a.Databases[collOne]

			gomega.Expect(len(db.DocCount)).To(gomega.Equal(1))

			for c := range db.DocCount {
				gomega.Expect(c <= blockMark).To(gomega.Equal(true))
				gomega.Expect(c != 0).To(gomega.Equal(true))
			}

			gomega.Expect(len(db.BlockTracker)).To(gomega.Equal(4))

			_, ok := db.BlockTracker[0]
			gomega.Expect(ok).To(gomega.Equal(true))

			_, ok = db.BlockTracker[1]
			gomega.Expect(ok).To(gomega.Equal(true))

			_, ok = db.BlockTracker[2]
			gomega.Expect(ok).To(gomega.Equal(true))

			_, ok = db.BlockTracker[3]
			gomega.Expect(ok).To(gomega.Equal(true))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should inspect block tracker replace", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")

		n := 3000

		ids := [blockMark]int{}
		for i := 0; i < n; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		for i := 0; i < 1500; i++ {
			res := testSingleReplace(ReplaceMetadata{
				CollectionName: collName,
				ID:             ids[i],
				Data:           testAsJson("update"),
			}, a)

			gomega.Expect(res.Status).To(gomega.Equal(ReplacedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReplaceMethodType))
		}

		db := a.Databases[collName]

		gomega.Expect(len(db.BlockTracker)).To(gomega.Equal(1))

		track := db.BlockTracker[0]

		gomega.Expect(track[1]).To(gomega.Equal(uint16(177)))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should assert block number based on different write numbers", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")

		n := 100000

		for i := 0; i < n; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
		}

		dirs, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collName))

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(dirs)).To(gomega.Equal(n/blockMark + 1))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should successfully perform and inspect inserts", func() {
		r := testCreateRose(false)

		collName := testCreateCollection(r, "coll")

		n := 10000

		m := r.Databases[collName]

		testMultipleConcurrentInsert(n, "", r, collName)

		assertIndexIntegrity(m, n)

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should successfully perform and inspect deletes", func() {
		r := testCreateRose(false)

		collName := testCreateCollection(r, "coll")
		const n = 10000

		ids := testMultipleConcurrentInsert(n, testAsJson("sdlčfjasdfjksaldf"), r, collName)

		// since block index starts at 0, expected must be 3
		assertIndexIntegrity(r.Databases[collName], n)

		zerosDeleted := 0
		wg := &sync.WaitGroup{}
		for _, id := range ids {
			wg.Add(1)

			go func(id int, zerosDeleted int, wg *sync.WaitGroup) {
				if id == 0 && zerosDeleted == 1 {
					return
				}

				if id == 0 && zerosDeleted == 0 {
					zerosDeleted++
				}

				res, err := r.Delete(DeleteMetadata{
					CollectionName: collName,
					ID:             id,
				})

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
				gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))

				wg.Done()
			}(id, zerosDeleted, wg)
		}

		wg.Wait()

		assertIndexIntegrity(r.Databases[collName], 0)

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should successfully perform and inspect delete reallocation", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll")
		n := 10000

		m := r.Databases[collName]

		ids := testMultipleConcurrentInsert(n, testAsJson("sdlčfjasdfjksaldf"), r, collName)

		assertIndexIntegrity(m, n)

		wg := &sync.WaitGroup{}
		for _, id := range ids {
			wg.Add(1)
			go func(id int, wg *sync.WaitGroup) {
				res, err := r.Delete(DeleteMetadata{
					CollectionName: collName,
					ID:             id,
				})

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
				gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))

				wg.Done()
			}(id, wg)
		}

		wg.Wait()

		gomega.Expect(m.AutoIncrementCounter).To(gomega.Equal(n + 1))
		assertIndexIntegrity(m, 0)

		n = 50000
		testMultipleConcurrentInsert(n, testAsJson("sdlčfjasdfjksaldf"), r, collName)

		gomega.Expect(m.AutoIncrementCounter).To(gomega.Equal(60000 + 1))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should create an index and write to it, then after restart, have the index in memory", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll")
		err := r.NewIndex(collName, "type", stringIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		n := 10000

		testMultipleConcurrentInsert(n, testAsJsonInterface(TestUser{
			Type:      "user",
			Email:     "mario@gmail.com",
			IsValid:   false,
			Price:     12.34,
			RandomNum: 67,
			CreatedAt: "",
			UpdatedAt: "",
		}), r, collName)

		if err := r.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		r = testCreateRose(false)

		m := r.Databases[collName]

		fieldIndex, ok := m.FieldIndex["type"]

		gomega.Expect(ok).To(gomega.Equal(true))
		gomega.Expect(fieldIndex.DataType).To(gomega.Equal(stringIndexType))
		gomega.Expect(len(fieldIndex.Index)).To(gomega.Equal(n))

		gomega.Expect(len(m.FieldIndexKeys)).To(gomega.Equal(1))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should create multiple indexes and write to it, then after restart, have the indexes in memory", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll")
		err := r.NewIndex(collName, "type", stringIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		err = r.NewIndex(collName, "email", stringIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		err = r.NewIndex(collName, "isValid", boolIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		err = r.NewIndex(collName, "price", floatIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		err = r.NewIndex(collName, "randomNum", intIndexType)

		gomega.Expect(err).To(gomega.BeNil())

		n := 10000

		testMultipleConcurrentInsert(n, testAsJsonInterface(TestUser{
			Type:      "user",
			Email:     "mario@gmail.com",
			IsValid:   false,
			Price:     12.34,
			RandomNum: 67,
			CreatedAt: "",
			UpdatedAt: "",
		}), r, collName)

		if err := r.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		r = testCreateRose(false)

		m := r.Databases[collName]

		typeFieldIndex, ok := m.FieldIndex["type"]

		gomega.Expect(ok).To(gomega.Equal(true))
		gomega.Expect(typeFieldIndex.DataType).To(gomega.Equal(stringIndexType))
		gomega.Expect(len(typeFieldIndex.Index)).To(gomega.Equal(n))

		emailFieldIndex, ok := m.FieldIndex["email"]

		gomega.Expect(ok).To(gomega.Equal(true))
		gomega.Expect(emailFieldIndex.DataType).To(gomega.Equal(stringIndexType))
		gomega.Expect(len(emailFieldIndex.Index)).To(gomega.Equal(n))

		isValidFieldIndex, ok := m.FieldIndex["isValid"]

		gomega.Expect(ok).To(gomega.Equal(true))
		gomega.Expect(isValidFieldIndex.DataType).To(gomega.Equal(boolIndexType))
		gomega.Expect(len(isValidFieldIndex.Index)).To(gomega.Equal(n))

		priceFieldIndex, ok := m.FieldIndex["price"]

		gomega.Expect(ok).To(gomega.Equal(true))
		gomega.Expect(priceFieldIndex.DataType).To(gomega.Equal(floatIndexType))
		gomega.Expect(len(priceFieldIndex.Index)).To(gomega.Equal(n))

		randomNumFieldIndex, ok := m.FieldIndex["randomNum"]

		gomega.Expect(ok).To(gomega.Equal(true))
		gomega.Expect(randomNumFieldIndex.DataType).To(gomega.Equal(intIndexType))
		gomega.Expect(len(randomNumFieldIndex.Index)).To(gomega.Equal(n))

		gomega.Expect(len(m.FieldIndexKeys)).To(gomega.Equal(5))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

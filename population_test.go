package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"io/ioutil"
	"math/rand"
	"time"
)

var _ = GinkgoDescribe("Population tests and integrity tests", func() {
	GinkgoIt("Should assert that the memory database is populated correctly from an existing fs database", func() {
		ginkgo.Skip("")

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)

		collOne := testCreateCollection(a, "collOne")
		collTwo := testCreateCollection(a, "collTwo")
		collThree := testCreateCollection(a, "collThree")

		firstN := 100000
		secondN := 234789
		thirdN := 1234

		// Insert first collection
		firstIds := [100000]int{}
		for i := 0; i < firstN; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collOne}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			firstIds[i] = res.ID
		}

		// Insert second collection
		secondIds := [234789]int{}
		for i := 0; i < secondN; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collTwo}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			secondIds[i] = res.ID
		}

		// Insert third collection
		thirdIds := [1234]int{}
		for i := 0; i < thirdN; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collThree}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			thirdIds[i] = res.ID
		}

		firstDirs, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collOne))
		gomega.Expect(err).To(gomega.BeNil())

		secondDirs, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collTwo))
		gomega.Expect(err).To(gomega.BeNil())

		thirdDirs, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collThree))
		gomega.Expect(err).To(gomega.BeNil())

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(firstDirs)).To(gomega.Equal(firstN / blockMark + 1))
		gomega.Expect(len(secondDirs)).To(gomega.Equal(secondN / blockMark + 1))
		gomega.Expect(len(thirdDirs)).To(gomega.Equal(thirdN / blockMark + 1))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = testCreateRose(false)

		// Test reads for collection one
		total := 0
		for _, id := range firstIds {
			s := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &s, CollectionName: collOne}, a)

			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			total++
		}

		gomega.Expect(total).To(gomega.Equal(len(firstIds)))

		// test reads for collection two
		total = 0
		for _, id := range secondIds {
			s := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &s, CollectionName: collTwo}, a)

			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			total++
		}

		gomega.Expect(total).To(gomega.Equal(len(secondIds)))

		// test reads for collection two
		total = 0
		for _, id := range thirdIds {
			s := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &s, CollectionName: collThree}, a)

			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			total++
		}

		gomega.Expect(total).To(gomega.Equal(len(thirdIds)))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should assert correct blocks are opened while deleting", func() {
		ginkgo.Skip("")

		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll_name")
		counter := 0

		firstWrite := [2500]int{}
		for i := 0; i < 2500; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			firstWrite[i] = res.ID
		}

		gomega.Expect(roseBlockFile(0, fmt.Sprintf("%s/%s", roseDbDir(), collName))).To(gomega.Equal(a.Databases[collName].WriteDriver.Handler.File.Name()))

		secondWrite := [501]int{}
		for i := 2501; i < 3002; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			secondWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(0, fmt.Sprintf("%s/%s", roseDbDir(), collName))).To(gomega.Equal(a.Databases[collName].WriteDriver.Handler.File.Name()))

		counter = 0
		thirdWrite := [3000]int{}
		for i := 3002; i < 6002; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			thirdWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(1, fmt.Sprintf("%s/%s", roseDbDir(), collName))).To(gomega.Equal(a.Databases[collName].WriteDriver.Handler.File.Name()))

		counter = 0
		fourthWrite := [3000]int{}
		for i := 6002; i < 9002; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			fourthWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(2, fmt.Sprintf("%s/%s", roseDbDir(), collName))).To(gomega.Equal(a.Databases[collName].WriteDriver.Handler.File.Name()))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range firstWrite {
			res := testSingleDelete(DeleteMetadata{ID: id, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(0, fmt.Sprintf("%s/%s", roseDbDir(), collName))).To(gomega.Equal(a.Databases[collName].DeleteDriver.Handler.File.Name()))

		for _, id := range secondWrite {
			res := testSingleDelete(DeleteMetadata{ID: id, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(0, fmt.Sprintf("%s/%s", roseDbDir(), collName))).To(gomega.Equal(a.Databases[collName].DeleteDriver.Handler.File.Name()))

		for _, id := range thirdWrite {
			res := testSingleDelete(DeleteMetadata{ID: id, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(1, fmt.Sprintf("%s/%s", roseDbDir(), collName))).To(gomega.Equal(a.Databases[collName].DeleteDriver.Handler.File.Name()))

		for _, id := range fourthWrite {
			res := testSingleDelete(DeleteMetadata{ID: id, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(2, fmt.Sprintf("%s/%s", roseDbDir(), collName))).To(gomega.Equal(a.Databases[collName].DeleteDriver.Handler.File.Name()))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should skip the deleted entries when booting a populated database", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)

		collName := testCreateCollection(a, "coll")
		n := 10000
		s := testAsJson(testString)

		ids := [10000]int{}
		for i := 0; i < n; i++ {
			res, err := a.Write(WriteMetadata{Data: s, CollectionName: collName})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range ids {
			// skip one
			if id == 1 {
				continue
			}

			res := testSingleDelete(DeleteMetadata{ID: id, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		count := 0
		for _, id := range ids {
			t := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &t, CollectionName: collName}, a)

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

				count++
			}
		}

		gomega.Expect(count).To(gomega.Equal(1))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		count = 0
		for _, id := range ids {
			t := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &t, CollectionName: collName}, a)

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

				count++
			}
		}

		gomega.Expect(count).To(gomega.Equal(1))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should skip the deleted entries when booting a populated database and strategically removing entries in the database", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")
		n := 4000
		s := testAsJson(testString)

		ids := [4000]int{}
		for i := 0; i < n; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		strategy := []int{0, 10, 150, 987, 1000, 1001, 1002, 3000, 3001, 3002, 1, 3998, 3999, 2367}

		for _, key := range strategy {
			id := ids[key]
			res := testSingleDelete(DeleteMetadata{ID: id, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		count := 0
		for _, id := range ids {
			t := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &t, CollectionName: collName}, a)

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

				count++
			}
		}

		gomega.Expect(count).To(gomega.Equal(n - len(strategy)))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, key := range strategy {
			id := ids[key]
			t := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &t, CollectionName: collName}, a)

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

	GinkgoIt("Should read all values with bytes disparity", func() {
		ginkgo.Skip("")

		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll")
		n := 10000

		max := 1000
		min := 1

		idValue := make(map[int]string)
		for i := 0; i < n; i++ {
			rand.Seed(time.Now().UnixNano())

			r := rand.Intn(max - min) + min

			val := ""
			for i := 0; i < r; i++ {
				val += fmt.Sprintf("%d", i)
			}

			res := testSingleConcurrentInsert(WriteMetadata{Data: testAsJson(val), CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			idValue[res.ID] = val
		}

		for id, value := range idValue {
			t := ""
			res := testSingleRead(ReadMetadata{ID: id, Data: &t, CollectionName: collName}, a)

			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			gomega.Expect(t).To(gomega.Equal(value))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

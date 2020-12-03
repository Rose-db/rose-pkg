package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"io/ioutil"
)

var _ = GinkgoDescribe("Population tests and integrity tests", func() {
	GinkgoIt("Should assert block number based on different write numbers", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		n := 100000

		for i := 0; i < n; i++ {
			res, err := a.Write(WriteMetadata{Data: s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
		}

		dirs, err := ioutil.ReadDir(roseDbDir())

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(dirs)).To(gomega.Equal(n / 3000 + 1))

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert that the memory database is populated correctly from an existing fs database", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		n := 100000

		ids := [100000]int{}
		for i := 0; i < n; i++ {
			res, err := a.Write(WriteMetadata{Data: s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		dirs, err := ioutil.ReadDir(roseDbDir())

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(dirs)).To(gomega.Equal(n / 3000 + 1))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = testCreateRose(false)
		total := 0
		for _, id := range ids {
			s := ""
			res, err := a.Read(ReadMetadata{ID: id, Data: &s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			total++
		}

		gomega.Expect(total).To(gomega.Equal(len(ids)))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should assert correct blocks are opened while deleting", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		counter := 0

		firstWrite := [2500]int{}
		for i := 0; i < 2500; i++ {
			res, err := a.Write(WriteMetadata{Data: s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			firstWrite[i] = res.ID
		}

		gomega.Expect(roseBlockFile(0, roseDbDir())).To(gomega.Equal(a.db.WriteDriver.Handler.File.Name()))

		secondWrite := [501]int{}
		for i := 2501; i < 3002; i++ {
			res, err := a.Write(WriteMetadata{Data: s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			secondWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(1, roseDbDir())).To(gomega.Equal(a.db.WriteDriver.Handler.File.Name()))

		counter = 0
		thirdWrite := [3000]int{}
		for i := 3002; i < 6002; i++ {
			res, err := a.Write(WriteMetadata{Data: s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			thirdWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(2, roseDbDir())).To(gomega.Equal(a.db.WriteDriver.Handler.File.Name()))

		counter = 0
		fourthWrite := [3000]int{}
		for i := 6002; i < 9002; i++ {
			res, err := a.Write(WriteMetadata{Data: s})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			fourthWrite[counter] = res.ID
			counter++
		}

		gomega.Expect(roseBlockFile(3, roseDbDir())).To(gomega.Equal(a.db.WriteDriver.Handler.File.Name()))

		if err := a.Shutdown(); err != nil {
			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(false)

		for _, id := range firstWrite {
			res, err := a.Delete(DeleteMetadata{ID: id})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(0, roseDbDir())).To(gomega.Equal(a.db.DeleteDriver.Handler.File.Name()))

		for _, id := range secondWrite {
			res, err := a.Delete(DeleteMetadata{ID: id})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(1, roseDbDir())).To(gomega.Equal(a.db.DeleteDriver.Handler.File.Name()))

		for _, id := range thirdWrite {
			res, err := a.Delete(DeleteMetadata{ID: id})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(2, roseDbDir())).To(gomega.Equal(a.db.DeleteDriver.Handler.File.Name()))

		for _, id := range fourthWrite {
			res, err := a.Delete(DeleteMetadata{ID: id})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		gomega.Expect(roseBlockFile(3, roseDbDir())).To(gomega.Equal(a.db.DeleteDriver.Handler.File.Name()))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should skip the deleted entries when booting a populated database", func() {
		a := testCreateRose(false)
		n := 1000
		s := testAsJson(testString)

		ids := [1000]int{}
		for i := 0; i < n; i++ {
			res, err := a.Write(WriteMetadata{Data: s})

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
			if id == 0 {
				continue
			}
			res, err := a.Delete(DeleteMetadata{ID: id})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))
		}

		count := 0
		for _, id := range ids {
			t := ""
			res, err := a.Read(ReadMetadata{ID: id, Data: &t})

			gomega.Expect(err).To(gomega.BeNil())

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
			res, err := a.Read(ReadMetadata{ID: id, Data: &t})

			gomega.Expect(err).To(gomega.BeNil())

			if res.Status == FoundResultStatus {
				gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

				count++
			}
		}

		gomega.Expect(count).To(gomega.Equal(1))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Shutdown failed with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should skip the deleted entries when booting a populated database and strategically removing entries in the database", func() {
		a := testCreateRose(false)
		n := 4000
		s := testAsJson(testString)

		ids := [4000]int{}
		for i := 0; i < n; i++ {
			res, err := a.Write(WriteMetadata{Data: s})

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

		strategy := []int{0, 10, 150, 987, 1000, 1001, 1002, 3000, 3001, 3002, 1, 3998, 3999, 2367}

		for _, key := range strategy {
			id := ids[key]
			res, err := a.Delete(DeleteMetadata{ID: id})

			gomega.Expect(err).To(gomega.BeNil())
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
			res, err := a.Read(ReadMetadata{ID: id, Data: &t})

			gomega.Expect(err).To(gomega.BeNil())

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
			res, err := a.Read(ReadMetadata{ID: id, Data: &t})

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
})

package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"io/ioutil"
	"os"
)

var _ = GinkgoDescribe("Misc tests", func() {
	GinkgoIt("Should return the real size of the database", func() {
		a := testCreateRose(false)

		files, err := ioutil.ReadDir(roseDbDir())

		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Could not calculate size of the database: %s", err.Error()))
		}

		var size uint64
		var dbSize uint64

		for _, f := range files {
			size += uint64(f.Size())
		}

		dbSize, err = a.Size()

		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Could not get size of the database: %s", err.Error()))
		}

		gomega.Expect(size).To(gomega.Equal(dbSize))
	})

	GinkgoIt("Rose should defragment after recreating it and not have deleted values in the database", func() {
		a := testCreateRose(false)
		n := 5000

		// write 5000
		ids := [5000]int{}
		for i := 0; i < n; i++ {
			s := testAsJson("some value")
			res, err := a.Write(WriteMetadata{
				CollectionName: "",
				Data:           s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			ids[i] = res.ID
		}

		delIds := [3000]int{}
		// delete 3000
		for i := 0; i < 3000; i++ {
			u := ids[i]

			res, err := a.Delete(DeleteMetadata{ID: u})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(DeletedResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(DeleteMethodType))

			delIds[i] = res.ID
		}

		if err := a.Shutdown(); err != nil {

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		a = nil

		a = testCreateRose(true)

		for i := 0; i < 3000; i++ {
			u := delIds[i]

			s := ""
			res, err := a.Read(ReadMetadata{
				ID:   u,
				Data: s,
			})

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(res.Status).To(gomega.Equal(NotFoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should create a new collection", func() {
		a := testCreateRose(false)
		collName := "some_collection"

		err := a.NewCollection(collName)

		gomega.Expect(err).To(gomega.BeNil())

		path := fmt.Sprintf("%s/%s", roseDbDir(), collName)
		stat, statErr := os.Stat(path)

		gomega.Expect(statErr).To(gomega.BeNil())

		gomega.Expect(stat.IsDir()).To(gomega.BeTrue())
		gomega.Expect(path).To(gomega.Equal(fmt.Sprintf("%s/%s", roseDbDir(), stat.Name())))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should not fail to create a collection because it exists", func() {
		a := testCreateRose(false)
		collName := "some_collection"

		err := a.NewCollection(collName)

		gomega.Expect(err).To(gomega.BeNil())

		path := fmt.Sprintf("%s/%s", roseDbDir(), collName)
		stat, statErr := os.Stat(path)

		gomega.Expect(statErr).To(gomega.BeNil())

		gomega.Expect(stat.IsDir()).To(gomega.BeTrue())
		gomega.Expect(path).To(gomega.Equal(fmt.Sprintf("%s/%s", roseDbDir(), stat.Name())))

		err = a.NewCollection(collName)

		gomega.Expect(err).To(gomega.BeNil())

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

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

		collOne := testCreateCollection(a, "coll_one")
		collTwo := testCreateCollection(a, "coll_two")
		collThree := testCreateCollection(a, "coll_three")

		testMultipleConcurrentInsert(10000, testAsJson("člksdjfčlkasjdflčjlsačdfj"), a, collOne)
		testMultipleConcurrentInsert(10000, testAsJson("člksdjfčlkasjdflčjlsačdfj"), a, collTwo)
		testMultipleConcurrentInsert(10000, testAsJson("člksdjfčlkasjdflčjlsačdfj"), a, collThree)

		filesOne, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collOne))
		gomega.Expect(err).To(gomega.BeNil())

		filesTwo, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collTwo))
		gomega.Expect(err).To(gomega.BeNil())

		filesThree, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", roseDbDir(), collThree))
		gomega.Expect(err).To(gomega.BeNil())

		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Could not calculate size of the database: %s", err.Error()))
		}

		var size uint64
		var dbSize uint64

		for _, f := range filesOne {
			size += uint64(f.Size())
		}

		for _, f := range filesTwo {
			size += uint64(f.Size())
		}

		for _, f := range filesThree {
			size += uint64(f.Size())
		}

		dbSize, err = a.Size()

		if err != nil {
			ginkgo.Fail(fmt.Sprintf("Could not get size of the database: %s", err.Error()))
		}

		gomega.Expect(size).To(gomega.Equal(dbSize))
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
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
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
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

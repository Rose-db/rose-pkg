package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"io/ioutil"
	"sync"
)

var _ = GinkgoDescribe("Internal Memory DB tests", func() {
	GinkgoIt("Should assert block number based on different write numbers", func() {
		s := testAsJson("sdčkfjalsčkjfdlsčakdfjlčk")
		a := testCreateRose(false)
		n := 100000

		for i := 0; i < n; i++ {
			res := testSingleConcurrentInsert(WriteMetadata{Data: s}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
		}

		dirs, err := ioutil.ReadDir(roseDbDir())

		gomega.Expect(err).To(gomega.BeNil())
		gomega.Expect(len(dirs)).To(gomega.Equal(n / 3000 + 1))

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect inserts", func() {
		r := testCreateRose(false)
		n := 10000

		m := r.db

		testMultipleConcurrentInsert(n, []uint8{}, r)

		// since block index starts at 0, expected must be 3
		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))
		assertIndexIntegrity(m, n)

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect deletes", func() {
		r := testCreateRose(false)
		const n = 10000

		m := r.db

		ids := testInsertFixture(m, n, []uint8{})

		// since block index starts at 0, expected must be 3
		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))
		assertIndexIntegrity(m, n)

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

				status, err := m.Delete(id)

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(status).To(gomega.Equal(true))

				wg.Done()
			}(id, zerosDeleted, wg)
		}

		wg.Wait()

		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))
		assertIndexIntegrity(m, 0)

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})

	GinkgoIt("Should successfully perform and inspect delete reallocation", func() {
		r := testCreateRose(false)
		n := 10000

		m := r.db

		ids := testInsertFixture(m,n, []uint8{})

		// since block index starts at 0, expected must be 3
		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))

		assertIndexIntegrity(m, n)

		wg := &sync.WaitGroup{}
		for _, id := range ids {
			wg.Add(1)
			go func(id int, wg *sync.WaitGroup) {
				status, err := m.Delete(id)

				gomega.Expect(err).To(gomega.BeNil())
				gomega.Expect(status).To(gomega.Equal(true))

				wg.Done()
			}(id, wg)
		}

		wg.Wait()

		gomega.Expect(m.AutoIncrementCounter).To(gomega.Equal(n))
		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(3)))
		assertIndexIntegrity(m, 0)

		n = 50000
		testInsertFixture(m,n, []uint8{})

		gomega.Expect(m.CurrMapIdx).To(gomega.Equal(uint16(20)))
		gomega.Expect(m.AutoIncrementCounter).To(gomega.Equal(60000))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb()

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb()
	})
})

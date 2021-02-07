package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = GinkgoDescribe("Index tests", func() {
	GinkgoIt("Assert that index cannot be created on a non existent collection", func() {
		a := testCreateRose(false)

		err := a.Index("not_exists", "some_field")

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(GenericMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))
		gomega.Expect(err.Error()).To(gomega.Equal("Invalid index request. Collection not_exists does not exist"))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})


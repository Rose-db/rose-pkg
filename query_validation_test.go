package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

var _ = GinkgoDescribe("Query validation tests", func() {
	GinkgoIt("Should fail if given invalid query field:index value", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")

		qb := NewQueryBuilder()

		err := qb.If(collName, "email == #email", map[string]interface{}{})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Unable to process query. Invalid field:index given. Field must be in field:index_type format. Index types are %v", fieldDataTypes)))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail if given invalid query index is not correct (not exists)", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")

		qb := NewQueryBuilder()

		err := qb.If(collName, "email:not_exists == #email", map[string]interface{}{})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Unable to process query. Invalid index given. Field must be in field:index_type format. Index formats are %v", fieldDataTypes)))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail if given invalid comparison operator", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")

		qb := NewQueryBuilder()

		err := qb.If(collName, "email:string &= #email", map[string]interface{}{})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Unable to process query. Invalid comparison operator given. Comparison operators are %v", comparisonOperators)))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail if consecutive groups have invalid conditional operators", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")

		qb := NewQueryBuilder()

		err := qb.If(collName, "email:string == mario@gmail.com !& type:string == user", map[string]interface{}{})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Unable to process query. Invalid conditional operator %s given. Valid conditional operators are %v", "!&", conditionalOperators)))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should fail if bound parameter is not provided", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")

		qb := NewQueryBuilder()

		err := qb.If(collName, "email:string == #email && type:string == user || type:date == user", map[string]interface{}{})

		gomega.Expect(err).To(gomega.Not(gomega.BeNil()))
		gomega.Expect(err.Error()).To(gomega.Equal(fmt.Sprintf("Unable to process query. Unable to find %s parameter in provided parameters", "#email")))
		gomega.Expect(err.GetMasterCode()).To(gomega.Equal(ValidationMasterErrorCode))
		gomega.Expect(err.GetCode()).To(gomega.Equal(InvalidUserSuppliedDataCode))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should be a success with all comparison and conditional operators", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")

		qb := NewQueryBuilder()

		err := qb.If(collName, "email:string == #email && type:string == user || type:date == user", map[string]interface{}{
			"#email": "mario@gmail.com",
		})

		gomega.Expect(err).To(gomega.BeNil())

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

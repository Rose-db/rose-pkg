package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"math/rand"
	"time"
)

type TestUser struct {
	Type string `json:"type"`
	Email string `json:"email"`
}

var _ = GinkgoDescribe("Query tests", func() {
	GinkgoIt("Should make an equality query", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")
		n := 10000

		emailList := []string{
			"mario@gmail.com",
			"mile@gmail.com",
			"zdravko@gmail.com",
			"miletina@gmail.com",
			"zdravkina@gmail.com",
		}

		rand.Seed(time.Now().UnixNano())

		writtenEmails := [5]int{}
		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
			}

			res := testSingleConcurrentInsert(WriteMetadata{
				CollectionName: collName,
				Data:           testAsJsonInterface(user),
			}, r)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			writtenEmails[rnd]++
		}

		for i, email := range emailList {
			qb := NewQueryBuilder()

			qb.If(collName, "email == :email", map[string]interface{}{
				":email": email,
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(err).To(gomega.BeNil())

			gomega.Expect(len(queryResults)).To(gomega.Equal(writtenEmails[i]))
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should make an equality without a placeholder", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")
		n := 10000

		emailList := []string{
			"mario@gmail.com",
			"mile@gmail.com",
			"zdravko@gmail.com",
			"miletina@gmail.com",
			"zdravkina@gmail.com",
		}

		rand.Seed(time.Now().UnixNano())

		writtenEmails := [5]int{}
		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
			}

			res := testSingleConcurrentInsert(WriteMetadata{
				CollectionName: collName,
				Data:           testAsJsonInterface(user),
			}, r)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			writtenEmails[rnd]++
		}

		qb := NewQueryBuilder()

		qb.If(collName, "email == mario@gmail.com", map[string]interface{}{})

		queryResults, err := r.Query(qb)

		gomega.Expect(err).To(gomega.BeNil())

		gomega.Expect(len(queryResults)).To(gomega.Equal(writtenEmails[0]))

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should make an inequality query", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")
		n := 10000

		emailList := []string{
			"mario@gmail.com",
			"mile@gmail.com",
			"zdravko@gmail.com",
			"miletina@gmail.com",
			"zdravkina@gmail.com",
		}

		rand.Seed(time.Now().UnixNano())

		writtenEmails := [5]int{}
		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
			}

			res := testSingleConcurrentInsert(WriteMetadata{
				CollectionName: collName,
				Data:           testAsJsonInterface(user),
			}, r)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			writtenEmails[rnd]++
		}

		for i, email := range emailList {
			qb := NewQueryBuilder()

			qb.If(collName, "email != :email", map[string]interface{}{
				":email": email,
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(err).To(gomega.BeNil())

			res := 0
			for j, num := range writtenEmails {
				if j != i {
					res += num
				}
			}

			gomega.Expect(len(queryResults)).To(gomega.Equal(res))
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should make an inequality query", func() {
		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")
		n := 10000

		emailList := []string{
			"mario@gmail.com",
			"mile@gmail.com",
			"zdravko@gmail.com",
			"miletina@gmail.com",
			"zdravkina@gmail.com",
		}

		rand.Seed(time.Now().UnixNano())

		writtenEmails := [5]int{}
		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
			}

			res := testSingleConcurrentInsert(WriteMetadata{
				CollectionName: collName,
				Data:           testAsJsonInterface(user),
			}, r)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			writtenEmails[rnd]++
		}

		for i, email := range emailList {
			qb := NewQueryBuilder()

			qb.If(collName, "email != :email", map[string]interface{}{
				":email": email,
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(err).To(gomega.BeNil())

			res := 0
			for j, num := range writtenEmails {
				if j != i {
					res += num
				}
			}

			gomega.Expect(len(queryResults)).To(gomega.Equal(res))
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should make an equality query with AND operator", func() {
		ginkgo.Skip("")

		r := testCreateRose(false)
		collName := testCreateCollection(r, "coll_name")
		n := 10000

		emailList := []string{
			"mario@gmail.com",
			"mile@gmail.com",
			"zdravko@gmail.com",
			"miletina@gmail.com",
			"zdravkina@gmail.com",
		}

		rand.Seed(time.Now().UnixNano())

		writtenEmails := [5]int{}
		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
			}

			res := testSingleConcurrentInsert(WriteMetadata{
				CollectionName: collName,
				Data:           testAsJsonInterface(user),
			}, r)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			writtenEmails[rnd]++
		}

		for _, email := range emailList {
			qb := NewQueryBuilder()

			qb.If(collName, "email == :email && type == company", map[string]interface{}{
				":email": email,
			})

			_, err := r.Query(qb)

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

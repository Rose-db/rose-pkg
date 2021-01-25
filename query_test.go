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
	IsValid bool `json:"isValid"`
	Price float64 `json:"price"`
	RandomNum int `json:"randomNum"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
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

			qb.If(collName, "email:string == #email", map[string]interface{}{
				"#email": email,
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

		qb.If(collName, "email:string == mario@gmail.com", map[string]interface{}{})

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

	GinkgoIt("Should make an inequality query with single condition", func() {
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

			qb.If(collName, "email:string != #email", map[string]interface{}{
				"#email": email,
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

	GinkgoIt("Should make an equality query with multiple operators but true only for OR", func() {
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
		}

		for _, email := range emailList {
			email = email
			qb := NewQueryBuilder()

			qb.If(collName, "email:string == #email && type:string == company || type:string == user", map[string]interface{}{
				"#email": "incorrect",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(5000))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should make an equality query with two block of AND operators", func() {
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
		}

		for _, email := range emailList {
			qb := NewQueryBuilder()

			qb.If(collName, "email:string == #email && type:string == #type || email:string == #email && type:string == #type", map[string]interface{}{
				"#email": email,
				"#type": "sdfjsadfjsldfasfd",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(0))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should make an equality query with two blocks of AND operators with a true OR operator", func() {
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
		}

		for _, email := range emailList {
			qb := NewQueryBuilder()

			qb.If(collName, "email:string == #email && type:string == #type || email:string == #email && type:string == #type || type:string == user", map[string]interface{}{
				"#email": email,
				"#type": "sdfjsadfjsldfasfd",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(5000))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should make an equality query with only OR operators", func() {
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
		}

		for _, email := range emailList {
			email = email

			qb := NewQueryBuilder()

			qb.If(collName, "type:string == sdfksdf || type:string == sdfdfd || type:string == company", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(5000))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should return no results with comparison operator", func() {
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

		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			o := false
			if i % 4 == 0 {
				o = true
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
				IsValid: o,
				Price: 2.35,
				RandomNum: 5,
			}

			res := testSingleConcurrentInsert(WriteMetadata{
				CollectionName: collName,
				Data:           testAsJsonInterface(user),
			}, r)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))
		}

		for _, email := range emailList {
			email = email

			qb := NewQueryBuilder()

			qb.If(collName, "price:float >= 1.23 && randomNum:int > 5", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(0))

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

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

			if err != (*queryError)(nil) {
				panic(err)
			}

			gomega.Expect(len(queryResults)).To(gomega.Equal(writtenEmails[i]))
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

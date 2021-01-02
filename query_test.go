package rose

import (
	"fmt"
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
		a := testCreateRose(false)
		collName := testCreateCollection(a, "coll_name")
		n := 100000

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
			r := rand.Intn(len(emailList) - 1)

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[r],
			}

			res := testSingleConcurrentInsert(WriteMetadata{
				CollectionName: collName,
				Data:           testAsJsonInterface(user),
			}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			writtenEmails[r]++
		}

		for i, email := range emailList {
			qb := NewQueryBuilder()

			qb.Do(collName, fmt.Sprintf("email == %s", email), "string")

			queryResults, err := a.Query(qb)

			if err != (*queryError)(nil) {
				panic(err)
			}

			gomega.Expect(len(queryResults)).To(gomega.Equal(writtenEmails[i]))
		}

	})
})

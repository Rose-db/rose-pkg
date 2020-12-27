package rose

import (
	"fmt"
	"github.com/bxcodec/faker"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"math/rand"
	"time"
)

type TestUser struct {
	Name string `json:"name" faker:"first_name"`
	LastName string `json:"lastName" faker:"last_name"`
	Email string `json:"email"`
}

var _ = GinkgoDescribe("Query tests", func() {
	GinkgoIt("Should query the data un mass", func() {
		testEmails := []string{
			"mario@gmail.com",
			"joanne@gmail.com",
			"kristina@gmail.com",
			"florentina@gmail.com",
			"mistifina@gmail.com",
			"julianne@gmail.com",
			"hanssina@gmail.com",
			"planetina@gmail.com",
			"crazyina@gmai.com",
			"collenne@gmail.com",
		}

		a := testCreateRose(false)

		collName := testCreateCollection(a, "test_coll")

		randomEmails := [10]int{}
		for i := 0; i < 10; i++ {
			rand.Seed(time.Now().UnixNano())

			r := rand.Intn((len(testEmails) - 1) - 0) + 0

			user := TestUser{}
			err := faker.FakeData(&user)

			gomega.Expect(err).To(gomega.BeNil())

			user.Email = testEmails[r]

			res := testSingleConcurrentInsert(WriteMetadata{
				CollectionName: collName,
				Data:           testAsJsonInterface(user),
			}, a)

			gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

			randomEmails[r]++
		}

		foundEmails := [10]int{}
		for i := 1; i < 11; i++ {
			user := TestUser{}
			res := testSingleRead(ReadMetadata{
				CollectionName: collName,
				ID:             i,
				Data:           &user,
			}, a)

			gomega.Expect(res.Status).To(gomega.Equal(FoundResultStatus))
			gomega.Expect(res.Method).To(gomega.Equal(ReadMethodType))

			email := user.Email

			for j := 0; j < len(testEmails); j++ {
				if testEmails[j] == email {
					foundEmails[j]++

					break
				}
			}
		}

		for i := 0; i < len(testEmails); i++ {
			r := randomEmails[i]
			f := foundEmails[i]

			gomega.Expect(r).To(gomega.Equal(f))
		}

		ch := make(chan bool)
		for i := 0; i < len(testEmails); i++ {
			go func(ch chan bool, index int, total int) {
				qb := NewQueryBuilder()
				qb.If(NewEqual(collName, "email", testEmails[index], stringType))

				queryResult, err := a.Query(qb)

				gomega.Expect(err).To(gomega.BeNil())

				fmt.Println(testEmails[index], len(queryResult), total)

				gomega.Expect(len(queryResult)).To(gomega.Equal(total))

				ch<- true
			}(ch, i, randomEmails[i])

			<-ch
		}


		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

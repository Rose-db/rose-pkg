package rose

import (
	"fmt"
	"github.com/bxcodec/faker"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"math/rand"
	"sync"
	"time"
)

type TestUser struct {
	Name string `json:"name" faker:"first_name"`
	LastName string `json:"lastName" faker:"last_name"`
	Email string `json:"email"`
}

var _ = GinkgoDescribe("Query tests", func() {
	GinkgoIt("Should query the data un mass simultaneously", func() {
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

		rand.Seed(time.Now().UnixNano())

		randomEmails := [10]int{}
		for i := 0; i < 200000; i++ {
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
		for i := 1; i < 200001; i++ {
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

		wg := &sync.WaitGroup{}
		for i := 0; i < len(testEmails); i++ {
			wg.Add(1)

			go func(wg *sync.WaitGroup, index int, total int) {
				qb := NewQueryBuilder()
				qb, err := qb.If(NewQuery(collName, "email", testEmails[index], stringType, ""))

				gomega.Expect(err).To(gomega.BeNil())

				queryResult, err := a.Query(qb)

				gomega.Expect(err).To(gomega.BeNil())

				gomega.Expect(len(queryResult)).To(gomega.Equal(total))

				wg.Done()
			}(wg, i, randomEmails[i])
		}

		wg.Wait()

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should query the data un mass synchronously", func() {
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

		rand.Seed(time.Now().UnixNano())

		randomEmails := [10]int{}
		for i := 0; i < 200000; i++ {
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

		ch := make(chan bool)
		for i := 0; i < len(testEmails); i++ {
			go func(ch chan bool, index int, total int) {
				qb := NewQueryBuilder()
				qb, err := qb.If(NewQuery(collName, "email", testEmails[index], stringType, ""))

				gomega.Expect(err).To(gomega.BeNil())

				queryResult, err := a.Query(qb)

				gomega.Expect(err).To(gomega.BeNil())

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

	GinkgoIt("Should query the data from multiple collections concurrently", func() {
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

		collOne := testCreateCollection(a, "coll_one")
		collTwo := testCreateCollection(a, "coll_two")

		rand.Seed(time.Now().UnixNano())

		colls := [2]string{collOne, collTwo}

		for _, collName := range colls {
			randomEmails := [10]int{}
			for i := 0; i < 200000; i++ {
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

			wg := &sync.WaitGroup{}
			for i := 0; i < len(testEmails); i++ {
				wg.Add(1)

				go func(wg *sync.WaitGroup, index int, total int) {
					qb := NewQueryBuilder()
					qb, err := qb.If(NewQuery(collName, "email", testEmails[index], stringType, ""))

					gomega.Expect(err).To(gomega.BeNil())

					queryResult, err := a.Query(qb)

					gomega.Expect(err).To(gomega.BeNil())

					gomega.Expect(len(queryResult)).To(gomega.Equal(total))

					wg.Done()
				}(wg, i, randomEmails[i])
			}

			wg.Wait()
		}

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should perform queries with producer/consumer pattern", func() {
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

		collOne := testCreateCollection(a, "coll_one")

		ch := make(chan string)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for c := range ch {
				qb := NewQueryBuilder()
				qb, err := qb.If(NewQuery(collOne, "email", c, stringType, ""))

				gomega.Expect(err).To(gomega.BeNil())

				queryResult, err := a.Query(qb)

				gomega.Expect(err).To(gomega.BeNil())

				gomega.Expect(len(queryResult)).To(gomega.Not(gomega.Equal(0)))
			}

			wg.Done()
		}()

		go func(ch chan string) {
			for i := 0; i < 3000; i++ {
				r := rand.Intn((len(testEmails) - 1) - 0) + 0

				user := TestUser{}
				err := faker.FakeData(&user)

				gomega.Expect(err).To(gomega.BeNil())

				user.Email = testEmails[r]

				res := testSingleConcurrentInsert(WriteMetadata{
					CollectionName: collOne,
					Data:           testAsJsonInterface(user),
				}, a)

				gomega.Expect(res.Status).To(gomega.Equal(OkResultStatus))
				gomega.Expect(res.Method).To(gomega.Equal(WriteMethodType))

				ch<- user.Email
			}

			close(ch)
		}(ch)

		wg.Wait()

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should query values with AND statement from the same collection", func() {
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

		rand.Seed(time.Now().UnixNano())

		randomEmails := [10]int{}
		for i := 0; i < 200000; i++ {
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

		qb := NewQueryBuilder()

		qb, err := qb.If(NewAnd(
			NewQuery(collName, "email", "collenne@gmail.com", "string", ""),
			NewQuery(collName, "email", "mistifina@gmail.com", "string", ""),
		))

		gomega.Expect(err).To(gomega.BeNil())

		queryResults, err := a.Query(qb)

		gomega.Expect(err).To(gomega.BeNil())

		total := randomEmails[9] + randomEmails[4]

		gomega.Expect(total).To(gomega.Equal(len(queryResults)))

		if err := a.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})
})

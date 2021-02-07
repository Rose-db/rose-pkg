package rose

import (
	"fmt"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"math/rand"
	"time"
)

var _ = GinkgoDescribe("Query comparison tests", func() {
	GinkgoIt("Should use equality query with boolean values", func() {
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

			qb.If(collName, "isValid:bool == true", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use inequality query with boolean values", func() {
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

			qb.If(collName, "isValid:bool != false", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use equality operator on the float type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float == 2.35", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use inequality operator on the float type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float != 2.34", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use more comparison operator on the float type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float > 1.23", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use less comparison operator on the float type", func() {
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

		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
				IsValid: true,
				Price: 2.35,
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

			qb.If(collName, "isValid:bool == true && price:float < 2.36", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use lessEqual comparison operator on the float type", func() {
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

		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
				IsValid: true,
				Price: 2.35,
			}

			if i == 0 {
				user = &TestUser{
					Type:  t,
					Email: emailList[rnd],
					IsValid: true,
					Price: 2.36,
				}
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

			qb.If(collName, "isValid:bool == true && price:float <= 2.36", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use moreEqual comparison operator on the float type", func() {
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

		for i := 0; i < n; i++ {
			rnd := rand.Intn(len(emailList))

			t := "company"
			if i % 2 == 0 {
				t = "user"
			}

			user := &TestUser{
				Type:  t,
				Email: emailList[rnd],
				IsValid: true,
				Price: 2.35,
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

			qb.If(collName, "isValid:bool == true && price:float >= 2.35", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use equality comparison operator on the integer type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float > 1.23 && randomNum:int == 5", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use inequality comparison operator on the integer type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float > 1.23 && randomNum:int != 6", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use less comparison operator on the integer type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float < 2.36 && randomNum:int < 6", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use more comparison operator on the integer type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float < 2.36 && randomNum:int > 4", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use lessEqual comparison operator on the integer type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float < 2.36 && randomNum:int <= 5", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use moreEqual comparison operator on the integer type", func() {
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

			qb.If(collName, "isValid:bool == true && price:float < 2.36 && randomNum:int >= 5", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n / 4))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use equality comparison operator on the date type", func() {
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
				CreatedAt: "2019-3-12",
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || createdAt:date == 2019-3-12", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use inequality comparison operator on the date type", func() {
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
				CreatedAt: "2019-3-12",
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || createdAt:date != 2018-3-12", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use less comparison operator on the date type", func() {
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
				CreatedAt: "2019-3-12",
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || createdAt:date < 2020-3-12", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use more comparison operator on the date type", func() {
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
				CreatedAt: "2019-3-12",
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || createdAt:date > 2018-03-12", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use lessEqual comparison operator on the date type", func() {
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
				CreatedAt: "2019-3-12",
			}

			if i == 0 {
				user = &TestUser{
					Type:  t,
					Email: emailList[rnd],
					IsValid: o,
					Price: 2.35,
					RandomNum: 5,
					CreatedAt: "2019-3-11",
				}
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || createdAt:date <= 2019-03-12", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use moreEqual comparison operator on the date type", func() {
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
				CreatedAt: "2019-3-12",
			}

			if i == 0 {
				user = &TestUser{
					Type:  t,
					Email: emailList[rnd],
					IsValid: o,
					Price: 2.35,
					RandomNum: 5,
					CreatedAt: "2019-3-14",
				}
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || createdAt:date >= 2019-03-12", map[string]interface{}{})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use equality comparison operator on the date_time type", func() {
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
				CreatedAt: "2019-3-12 12:34:56",
				UpdatedAt: "2019-3-12 12:34:56",
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || updatedAt:date_time == #updatedAt", map[string]interface{}{
				"#updatedAt": "2019-3-12 12:34:56",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use inequality comparison operator on the date_time type", func() {
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
				CreatedAt: "2019-3-12 12:34:56",
				UpdatedAt: "2019-3-12 12:34:56",
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || updatedAt:date_time != #updatedAt", map[string]interface{}{
				"#updatedAt": "2019-3-12 12:34:55",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use less comparison operator on the date_time type", func() {
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
				CreatedAt: "2019-3-12 12:34:56",
				UpdatedAt: "2019-3-12 12:34:56",
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || updatedAt:date_time < #updatedAt", map[string]interface{}{
				"#updatedAt": "2019-3-12 12:34:57",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use more comparison operator on the date_time type", func() {
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
				CreatedAt: "2019-3-12 12:34:56",
				UpdatedAt: "2019-3-12 12:34:56",
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || updatedAt:date_time > #updatedAt", map[string]interface{}{
				"#updatedAt": "2019-3-12 12:34:53",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use lessEqual comparison operator on the date_time type", func() {
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
				CreatedAt: "2019-3-12 12:34:56",
				UpdatedAt: "2019-3-12 12:34:56",
			}

			if i == 0 {
				user = &TestUser{
					Type:  t,
					Email: emailList[rnd],
					IsValid: o,
					Price: 2.35,
					RandomNum: 5,
					CreatedAt: "2019-3-12 12:34:56",
					UpdatedAt: "2019-3-12 12:34:55",
				}
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || updatedAt:date_time <= #updatedAt", map[string]interface{}{
				"#updatedAt": "2019-3-12 12:34:56",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

			gomega.Expect(err).To(gomega.BeNil())
		}

		if err := r.Shutdown(); err != nil {
			testRemoveFileSystemDb(roseDir())

			ginkgo.Fail(fmt.Sprintf("Rose failed to shutdown with message: %s", err.Error()))

			return
		}

		testRemoveFileSystemDb(roseDir())
	})

	GinkgoIt("Should use moreEqual comparison operator on the date_time type", func() {
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
				CreatedAt: "2019-3-12 12:34:56",
				UpdatedAt: "2019-3-12 12:34:56",
			}

			if i == 0 {
				user = &TestUser{
					Type:  t,
					Email: emailList[rnd],
					IsValid: o,
					Price: 2.35,
					RandomNum: 5,
					CreatedAt: "2019-3-12 12:34:56",
					UpdatedAt: "2019-3-12 12:34:57",
				}
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

			qb.If(collName, "isValid:bool == false && price:float == 2.36 && randomNum:int >= 6 || updatedAt:date_time >= #updatedAt", map[string]interface{}{
				"#updatedAt": "2019-3-12 12:34:56",
			})

			queryResults, err := r.Query(qb)

			gomega.Expect(len(queryResults)).To(gomega.Equal(n))

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

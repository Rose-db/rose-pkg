package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestInvalidId(t *testing.T) {
	var iv []string
	var m *Metadata
	var a *Rose

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	iv = []string{"insert", "read", "delete"}

	for i := 0; i < len(iv); i++ {
		m = &Metadata{
			Data:   []uint8{},
			Id: "",
		}

		_, err := a.Insert(m)

		if err != nil {
			if err.GetCode() != HttpErrorCode {
				t.Errorf("%s: Invalid error code given. Expected %d, got %d", testGetTestName(t), HttpErrorCode, err.GetCode())
			}
		}

	}

	a.Shutdown()
}

func TestSingleInsert(t *testing.T) {
	var s []uint8
	var a *Rose
	var m *Metadata

	var runErr RoseError
	var appResult *AppResult

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	s = []uint8("sdčkfjalsčkjfdlsčakdfjlčk")

	m = &Metadata{
		Data:   s,
		Id:     "id",
	}

	appResult, runErr = a.Insert(m)

	assertSuccessfulInsertResult(runErr, appResult, t)

	if appResult.Id != 0 {
		t.Errorf("%s: Rose::Run invalid Id returned on inisert. Got %d, expected %d", testGetTestName(t), appResult.Id, 0)

		return
	}

	a.Shutdown()
}

func TestMultipleInsert(t *testing.T) {
	var s []uint8
	var a *Rose
	var m *Metadata

	var appErr RoseError
	var appResult *AppResult
	var currId uint64

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	for i := 0; i < 50000; i++ {
		s = []uint8("sdčkfjalsčkjfdlsčakdfjlčk")

		m = &Metadata{
			Data:   s,
			Id:     fmt.Sprintf("id-%d", i),
		}

		appResult, appErr = a.Insert(m)

		assertSuccessfulInsertResult(appErr, appResult, t)

		if appResult.Id != currId {
			t.Errorf("%s: Rose::Run() there has been a discrepancy between generated id and counted id. Got %d, expected %d", testGetTestName(t), appResult.Id, currId)

			return
		}

		currId++
	}

	a.Shutdown()
}

func TestSingleRead(t *testing.T) {
	var a *Rose
	var m *Metadata
	var runErr RoseError
	var appResult *AppResult

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	fixtureSingleInsert("id", "id value", a, t, testGetTestName(t))

	m = &Metadata{
		Id:     "id",
	}

	appResult, runErr = a.Read(m)

	if runErr != nil {
		t.Errorf("%s resulted in an error: %s", testGetTestName(t), runErr.Error())

		return
	}

	if appResult.Status != FoundResultStatus {
		t.Errorf("%s invalid result not-found status: %s", testGetTestName(t), appResult.Reason)

		return
	}

	if appResult.Result != "id value" {
		t.Errorf("%s invalid result: Got %s, expected %s", testGetTestName(t), appResult.Result, "id value")

		return
	}

	a.Shutdown()
}

func TestSingleReadNotFound(t *testing.T) {
	var a *Rose
	var m *Metadata
	var runErr RoseError
	var appResult *AppResult

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	m = &Metadata{
		Id:     "id",
	}

	appResult, runErr = a.Read(m)

	if runErr != nil {
		t.Errorf("%s resulted in an error: %s", testGetTestName(t), runErr.Error())

		return
	}

	if appResult.Status != NotFoundResultStatus {
		t.Errorf("%s invalid result: Expected %s, got %s", testGetTestName(t), NotFoundResultStatus, appResult.Status)

		return
	}

	a.Shutdown()
}

func TestConcurrentInserts(t *testing.T) {
	var a *Rose
	var m *Metadata

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	num := 100000
	idChan := make(chan string, num)

	for i := 0; i < num; i++ {
		go func(i int, idChan chan string) {
			s := []uint8("sdčkfjalsčkjfdlsčakdfjlčk")
			id := fmt.Sprintf("id-%d", i)

			m = &Metadata{
				Data:   s,
				Id:     id,
			}

			appResult, appErr := a.Insert(m)

			assertSuccessfulInsertResult(appErr, appResult, t)

			idChan<- id
		}(i, idChan)
	}

	for i := 0; i < num; i++ {
		c := <-idChan
		res, err := a.Read(&Metadata{
			Id:  c,
		})

		assertSuccessfulReadResult(err, res, t)
	}

	a.Shutdown()
}

func TestDeleteNotFound(t *testing.T) {
	var a *Rose
	var runErr RoseError
	var appResult *AppResult

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	appResult, runErr = a.Delete(&Metadata{
		Id: "id",
	})

	assertDeleteNotFoundResult(runErr, appResult, t)

	a.Shutdown()
}

func TestSingleDelete(t *testing.T) {
	var a *Rose
	var m *Metadata
	var runErr RoseError
	var appResult *AppResult

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	s := []uint8("sdčkfjalsčkjfdlsčakdfjlčk")

	m = &Metadata{
		Data:   s,
		Id:     "id",
	}

	appResult, runErr = a.Insert(m)

	assertSuccessfulInsertResult(runErr, appResult, t)

	if appResult.Id != 0 {
		t.Errorf("%s: Rose::Run invalid Id returned on inisert. Got %d, expected %d", testGetTestName(t), appResult.Id, 0)

		return
	}

	appResult, runErr = a.Delete(&Metadata{
		Id: "id",
	})

	assertSuccessfulDeleteResult(runErr, appResult, t)
}

func TestConcurrentDelete(t *testing.T) {
	var a *Rose
	var m *Metadata

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	num := 100000
	idChan := make(chan string, num)

	for i := 0; i < num; i++ {
		go func(i int, idChan chan string) {
			s := []uint8("sdčkfjalsčkjfdlsčakdfjlčk")
			id := fmt.Sprintf("id-%d", i)

			m = &Metadata{
				Data:   s,
				Id:     id,
			}

			appResult, appErr := a.Insert(m)

			assertSuccessfulInsertResult(appErr, appResult, t)

			idChan<- id
		}(i, idChan)
	}

	for i := 0; i < num; i++ {
		c := <-idChan
		res, err := a.Delete(&Metadata{
			Id:  c,
		})

		assertSuccessfulDeleteResult(err, res, t)
	}

	a.Shutdown()
}

func testCreateRose(testName string) *Rose {
	var a *Rose

	a = New(false)

	return a
}

func testRemoveFileSystemDb(t *testing.T) {
	var dir string

	dir = roseDbDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Errorf("%s: Database directory .rose_db was not created in %s", dir, testGetTestName(t))

		return
	}

	files, err := ioutil.ReadDir(dir)

	if err != nil {
		t.Errorf("%s: Removing %s failed with message %s", dir, testGetTestName(t), err.Error())

		return
	}

	for _, f := range files {
		err = os.Remove(fmt.Sprintf("%s/%s", dir, f.Name()))

		if err != nil {
			t.Errorf("%s: Removing %s failed with message %s", dir, testGetTestName(t), err.Error())

			return
		}
	}
}

func benchmarkRemoveFileSystemDb(b *testing.B) {
	var dir string

	dir = roseDbDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		b.Errorf("%s: Database directory .rose_db was not created in %s", dir, testGetBenchmarkName(b))

		return
	}

	files, err := ioutil.ReadDir(dir)

	if err != nil {
		b.Errorf("%s: Removing %s failed with message %s", dir, testGetBenchmarkName(b), err.Error())

		return
	}

	for _, f := range files {
		err = os.Remove(fmt.Sprintf("%s/%s", dir, f.Name()))

		if err != nil {
			b.Errorf("%s: Removing %s failed with message %s", dir, testGetBenchmarkName(b), err.Error())

			return
		}
	}
}

func testGetBenchmarkName(b *testing.B) string {
	v := reflect.ValueOf(*b)
	return v.FieldByName("name").String()
}

func testGetTestName(t *testing.T) string {
	v := reflect.ValueOf(*t)
	return v.FieldByName("name").String()
}

func fixtureSingleInsert(id string, value string, a *Rose, t *testing.T, testName string) {
	var s []uint8
	var m *Metadata
	var appErr RoseError
	s = []uint8(value)

	m = &Metadata{
		Data:   s,
		Id:     id,
	}

	_, appErr = a.Insert(m)

	if appErr != nil {
		panic(fmt.Sprintf("%s: fixtureInsertSingle: Rose failed to Init with message: %s", testName, appErr.Error()))
	}
}

func assertSuccessfulInsertResult(runErr RoseError, appResult *AppResult, t *testing.T) {
	if runErr != nil {
		t.Errorf("%s resulted in an error: %s", testGetTestName(t), runErr.Error())
	}

	if appResult.Status != OkResultStatus {
		t.Errorf("%s invalid result not-found status: %s", testGetTestName(t), appResult.Reason)
	}

	if appResult.Method != InsertMethodType {
		t.Errorf("%s invalid method: Got %s, Expected %s", testGetTestName(t), appResult.Method, InsertMethodType)
	}
}

func assertSuccessfulReadResult(runErr RoseError, appResult *AppResult, t *testing.T) {
	if runErr != nil {
		t.Errorf("%s resulted in an error: %s", testGetTestName(t), runErr.Error())
	}

	if appResult.Status != FoundResultStatus {
		t.Errorf("%s invalid result not-found status: %s", testGetTestName(t), appResult.Reason)
	}

	if appResult.Method != ReadMethodType {
		t.Errorf("%s invalid method: Got %s, Expected %s", testGetTestName(t), appResult.Method, InsertMethodType)
	}
}

func assertSuccessfulDeleteResult(runErr RoseError, appResult *AppResult, t *testing.T) {
	if runErr != nil {
		t.Errorf("%s resulted in an error: %s", testGetTestName(t), runErr.Error())
	}

	if appResult.Status != EntryDeletedStatus {
		t.Errorf("%s invalid result not-found status: %s", testGetTestName(t), appResult.Reason)
	}

	if appResult.Method != DeleteMethodType {
		t.Errorf("%s invalid method: Got %s, Expected %s", testGetTestName(t), appResult.Method, InsertMethodType)
	}
}

func assertDeleteNotFoundResult(runErr RoseError, appResult *AppResult, t *testing.T) {
	if runErr != nil {
		t.Errorf("%s: Rose::Delete error occurred. Got error with message: %s", testGetTestName(t), runErr.Error())

		return
	}

	if appResult.Method != DeleteMethodType {
		t.Errorf("%s: Rose::Delete Invalid method type. Got %s, Expected %s", testGetTestName(t), appResult.Method, DeleteMethodType)

		return
	}

	if appResult.Id != 0 {
		t.Errorf("%s: Rose::Delete Invalid id. Got %d, Expected 0", testGetTestName(t), appResult.Id)

		return
	}

	if appResult.Status != NotFoundResultStatus {
		t.Errorf("%s: Rose::Delete Invalid status. Got %s, Expected %s", testGetTestName(t), appResult.Status, EntryDeletedStatus)

		return
	}
}
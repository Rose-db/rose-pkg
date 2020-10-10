package rose

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestDatabaseDirCreated(t *testing.T) {
	var m *Metadata
	var a *Rose

	defer testRemoveFileSystemDb(t)
	a = testCreateRose(testGetTestName(t))

	m = &Metadata{
		Data:   []byte{},
		Id: "validid",
	}

	err, _ := a.Insert(m)

	if err != nil {
		t.Errorf("%s: ApplicationController::Run() returned an error: %s", testGetTestName(t), err.Error())

		return
	}

}

func TestInvalidId(t *testing.T) {
	var iv []string
	var m *Metadata
	var a *Rose

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	iv = []string{"insert", "read", "delete"}

	for i := 0; i < len(iv); i++ {
		m = &Metadata{
			Data:   []byte{},
			Id: "",
		}

		err, _ := a.Insert(m)

		if err.GetCode() != HttpErrorCode {
			t.Errorf("%s: Invalid error code given. Expected %d, got %d", testGetTestName(t), HttpErrorCode, err.GetCode())
		}
	}

	a.Shutdown()
}

func TestSingleInsert(t *testing.T) {
	var s []byte
	var a *Rose
	var m *Metadata

	var runErr RoseError
	var appResult *AppResult

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	s = []byte("sdčkfjalsčkjfdlsčakdfjlčk")

	m = &Metadata{
		Data:   s,
		Id:     "id",
	}

	runErr, appResult = a.Insert(m)

	if runErr != nil {
		t.Errorf("%s: Rose::Run returned an error: %s", testGetTestName(t), runErr.Error())

		return
	}

	if appResult.Status != "ok" {
		t.Errorf("%s: Rose::Run returned a non ok status but it should return ok", testGetTestName(t))

		return
	}

	if appResult.Id != 0 {
		t.Errorf("%s: Rose::Run invalid Id returned on inisert. Got %d, expected %d", testGetTestName(t), appResult.Id, 0)

		return
	}

	a.Shutdown()
}

func TestMultipleInsert(t *testing.T) {
	var s []byte
	var a *Rose
	var m *Metadata

	var appErr RoseError
	var appResult *AppResult
	var currId uint64

	defer testRemoveFileSystemDb(t)

	a = testCreateRose(testGetTestName(t))

	for i := 0; i < 50000; i++ {
		s = []byte("sdčkfjalsčkjfdlsčakdfjlčk")

		m = &Metadata{
			Data:   s,
			Id:     fmt.Sprintf("id-%d", i),
		}

		appErr, appResult = a.Insert(m)

		if appErr != nil {
			t.Errorf("%s: Rose::Run() returned an error: %s", testGetTestName(t), appErr.Error())

			return
		}

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

	runErr, appResult = a.Read(m)

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

	runErr, appResult = a.Read(m)

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
	var s []byte
	var m *Metadata
	var appErr RoseError
	s = []byte(value)

	m = &Metadata{
		Data:   s,
		Id:     id,
	}

	appErr, _ = a.Insert(m)

	if appErr != nil {
		panic(fmt.Sprintf("%s: fixtureInsertSingle: Rose failed to Init with message: %s", testName, appErr.Error()))
	}
}
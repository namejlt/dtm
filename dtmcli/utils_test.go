package dtmcli

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEP(t *testing.T) {
	skipped := true
	err := func() (rerr error) {
		defer P2E(&rerr)
		E2P(errors.New("err1"))
		skipped = false
		return nil
	}()
	assert.Equal(t, true, skipped)
	assert.Equal(t, "err1", err.Error())
	err = CatchP(func() {
		PanicIf(true, errors.New("err2"))
	})
	assert.Equal(t, "err2", err.Error())
	err = func() (rerr error) {
		defer func() {
			x := recover()
			assert.Equal(t, 1, x)
		}()
		defer P2E(&rerr)
		panic(1)
	}()
}

func TestTernary(t *testing.T) {
	assert.Equal(t, "1", OrString("", "", "1"))
	assert.Equal(t, "", OrString("", "", ""))
	assert.Equal(t, "1", If(true, "1", "2"))
	assert.Equal(t, "2", If(false, "1", "2"))
}

func TestMarshal(t *testing.T) {
	a := 0
	type e struct {
		A int
	}
	e1 := e{A: 10}
	m := map[string]int{}
	assert.Equal(t, "1", MustMarshalString(1))
	assert.Equal(t, []byte("1"), MustMarshal(1))
	MustUnmarshal([]byte("2"), &a)
	assert.Equal(t, 2, a)
	MustUnmarshalString("3", &a)
	assert.Equal(t, 3, a)
	MustRemarshal(&e1, &m)
	assert.Equal(t, 10, m["A"])
}

func TestSome(t *testing.T) {
	n := MustAtoi("123")
	assert.Equal(t, 123, n)

	err := CatchP(func() {
		MustAtoi("abc")
	})
	assert.Error(t, err)

	func1 := GetFuncName()
	assert.Equal(t, true, strings.HasSuffix(func1, "TestSome"))

	os.Setenv("IS_DOCKER", "1")
	s := MayReplaceLocalhost("http://localhost")
	assert.Equal(t, "http://host.docker.internal", s)
	os.Setenv("IS_DOCKER", "")
	s2 := MayReplaceLocalhost("http://localhost")
	assert.Equal(t, "http://localhost", s2)
}

func TestFatal(t *testing.T) {
	old := FatalExitFunc
	defer func() {
		FatalExitFunc = old
	}()
	FatalExitFunc = func() { panic(fmt.Errorf("fatal")) }
	err := CatchP(func() {
		LogIfFatalf(true, "")
	})
	assert.Error(t, err, fmt.Errorf("fatal"))
}

func TestCompatible(t *testing.T) {
	old := DBDriver
	DBDriver = DriverMysql
	assert.Equal(t, "? ?", makeSQLCompatible("? ?"))
	assert.Equal(t, "xa start 'xa1'", getXaSQL("start", "xa1"))
	DBDriver = DriverPostgres
	assert.Equal(t, "$1 $2", makeSQLCompatible("? ?"))
	assert.Equal(t, "begin", getXaSQL("start", "xa1"))
	DBDriver = old
}

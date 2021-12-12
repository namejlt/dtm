package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yedf/dtm/common"
	"github.com/yedf/dtm/dtmcli/dtmimp"
)

func TestUtils(t *testing.T) {
	common.MustLoadConfig()
	db := dbGet()
	db.NoMust()
	err := dtmimp.CatchP(func() {
		checkAffected(db.DB)
	})
	assert.Error(t, err)
}

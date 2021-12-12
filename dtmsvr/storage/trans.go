package storage

import (
	"time"

	"github.com/yedf/dtm/common"
	"github.com/yedf/dtm/dtmcli"
)

type TransGlobalStore struct {
	common.ModelBase
	Gid              string              `json:"gid"`
	TransType        string              `json:"trans_type"`
	Steps            []map[string]string `json:"steps" gorm:"-"`
	Payloads         []string            `json:"payloads" gorm:"-"`
	BinPayloads      [][]byte            `json:"-" gorm:"-"`
	Status           string              `json:"status"`
	QueryPrepared    string              `json:"query_prepared"`
	Protocol         string              `json:"protocol"`
	CommitTime       *time.Time
	FinishTime       *time.Time
	RollbackTime     *time.Time
	Options          string
	CustomData       string `json:"custom_data"`
	NextCronInterval int64
	NextCronTime     *time.Time
	dtmcli.TransOptions
}

// TableName TableName
func (*TransGlobalStore) TableName() string {
	return "dtm.trans_global"
}

// TransBranchStore branch transaction
type TransBranchStore struct {
	common.ModelBase
	Gid          string
	URL          string `json:"url"`
	BinData      []byte
	BranchID     string `json:"branch_id"`
	Op           string
	Status       string
	FinishTime   *time.Time
	RollbackTime *time.Time
}

// TableName TableName
func (*TransBranchStore) TableName() string {
	return "dtm.trans_branch_op"
}

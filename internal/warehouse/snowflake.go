package warehouse

import (
	"database/sql"

	"github.com/snowflakedb/gosnowflake"
)

type SnowflakeClient struct {
	db *sql.DB
}

func NewSnowflake() *SnowflakeClient {
	return &SnowflakeClient{}
}

func (sc *SnowflakeClient) Connect(sfc SnowflakeCredentials) error {
	// Create the db instance
	dsn, err := sfc.ConnString()
	if err != nil {
		return err
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return err
	}

	// Configure the default snowflake logger, which is annoying.
	sflog := gosnowflake.GetLogger()
	// We basically don't want logs out of this. Everything relevant we get as
	// an error message that we log ourselves.
	sflog.SetLogLevel("panic")

	sc.db = db
	return nil
}

func (sc *SnowflakeClient) Run(query string) (Records, error) {
	// If we've not initialised the connection, return an empty result set for
	// now.
	// TODO this is clearly not ideal, if the warehouse isn't available the
	// server shouldn't even bother trying to serve queries and should just let
	// the caller know what's up.
	if sc.db == nil {
		return Records{}, nil
	}

	// Run the query
	rs, err := sc.db.Query(query)
	if err != nil {
		return Records{}, err
	}

	// If no results, return am empty record set
	if rs == nil {
		return Records{}, nil
	}

	// Otherwise convert to records
	records, err := rowsToRecords(rs)
	if err != nil {
		return Records{}, err
	}

	return records, nil
}

type SnowflakeCredentials struct {
	AccountId string `json:"account_id"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Database  string `json:"database"`
	Schema    string `json:"schema"`
	Warehouse string `json:"warehouse"`
}

func (sfc SnowflakeCredentials) ConnString() (string, error) {
	cfg := gosnowflake.Config{
		Account:   sfc.AccountId,
		User:      sfc.User,
		Password:  sfc.Password,
		Database:  sfc.Database,
		Schema:    sfc.Schema,
		Warehouse: sfc.Warehouse,
	}
	dsn, err := gosnowflake.DSN(&cfg)
	if err != nil {
		return "", err
	}
	return dsn, nil
}

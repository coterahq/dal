package warehouse

import (
	"database/sql"
	"regexp"

	"github.com/snowflakedb/gosnowflake"
	"github.com/supasheet/dal/internal/dal"
)

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

type SnowflakeClient struct {
	db    *sql.DB
	creds SnowflakeCredentials
}

func NewSnowflake(sfc SnowflakeCredentials) *SnowflakeClient {
	return &SnowflakeClient{creds: sfc}
}

func (sc *SnowflakeClient) Connect() error {
	// Create the db instance
	dsn, err := sc.creds.ConnString()
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

func (sc *SnowflakeClient) MapType(t string) dal.Scalar {
	return snowflakeDataTypeMatcher.match(t)
}

var snowflakeDataTypeMatcher = &dataTypeMatcher{
	id:       regexp.MustCompile("a^"),
	int:      regexp.MustCompile("(?i)(FIXED|INT|NUMBER|NUMERIC|DECIMAL)"),
	float:    regexp.MustCompile("(?i)(REAL|FLOAT|DOUBLE)"),
	boolean:  regexp.MustCompile("(?i)(BOOLEAN)"),
	string:   regexp.MustCompile("(?i)(CHAR|STRING|TEXT)"),
	dateTime: regexp.MustCompile("(?i)(TIME|DATE)"),
}

package warehouse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/supasheet/dal/internal/warehouse"
)

func TestConnString(t *testing.T) {
	type tc struct {
		creds       warehouse.SnowflakeCredentials
		expectation string
	}
	cases := []tc{
		{
			cred("acc.eu-west-1", "user", "pw", "db", "schema", "wh"),
			"user:pw@acc.eu-west-1.snowflakecomputing.com:443?database=db&ocspFailOpen=true&region=eu-west-1&schema=schema&validateDefaultParameters=true&warehouse=wh",
		},
		{
			cred("acc.eu-west-1", "user", "pw", "db", "schema", ""),
			"user:pw@acc.eu-west-1.snowflakecomputing.com:443?database=db&ocspFailOpen=true&region=eu-west-1&schema=schema&validateDefaultParameters=true",
		},
		{
			cred("acc.eu-west-1", "user", "pw", "db", "", "wh"),
			"user:pw@acc.eu-west-1.snowflakecomputing.com:443?database=db&ocspFailOpen=true&region=eu-west-1&validateDefaultParameters=true&warehouse=wh",
		},
		{
			cred("acc.eu-west-1", "user", "pw", "db", "", ""),
			"user:pw@acc.eu-west-1.snowflakecomputing.com:443?database=db&ocspFailOpen=true&region=eu-west-1&validateDefaultParameters=true",
		},
	}
	for _, c := range cases {
		t.Run(c.expectation, func(t *testing.T) {
			dsn, err := c.creds.ConnString()
			require.NoError(t, err)
			assert.Equal(t, c.expectation, dsn)
		})
	}
}

func cred(accountId, user, pw, db, schema, wh string) warehouse.SnowflakeCredentials {
	return warehouse.SnowflakeCredentials{
		AccountId: accountId, User: user,
		Password: pw, Database: db, Schema: schema, Warehouse: wh,
	}
}

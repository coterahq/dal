package e2e

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/supasheet/dal/internal/warehouse"
)

func setupSnowflake(t *testing.T) warehouse.Client {
	SkipE2E(t)

	c := os.Getenv("E2E_SNOWFLAKE_CREDENTIALS")
	decoded, err := base64.StdEncoding.DecodeString(c)
	require.NoError(t, err)

	// Parse json
	var creds warehouse.SnowflakeCredentials
	err = json.Unmarshal(decoded, &creds)
	require.NoError(t, err)
	sc := warehouse.NewSnowflake(creds)

	// Connect
	err = sc.Connect()
	require.NoError(t, err)

	return sc
}

func TestSnowflake_Query(t *testing.T) {
	client := setupSnowflake(t)
	rs, err := client.Run("select 1 as a, 2 as b;")
	require.NoError(t, err)

	expectation := warehouse.Records{
		warehouse.Record{"a": "1", "b": "2"},
	}

	assert.Equal(t, expectation, rs)
}

func TestSnowflake_QueryTwoRows(t *testing.T) {
	client := setupSnowflake(t)
	rs, err := client.Run("select 0 as a, 2 as b union select 1 as a, 3 as b;")

	require.NoError(t, err)

	expectation := warehouse.Records{
		warehouse.Record{"a": "0", "b": "2"},
		warehouse.Record{"a": "1", "b": "3"},
	}

	assert.Equal(t, expectation, rs)
}

func TestSnowflake_QueryMultipleRows(t *testing.T) {
	client := setupSnowflake(t)
	rs, err := client.Run(
		"select 0 as a, 4 as b union select 1 as a, 5 as b union select 2 as a, 6 as b union select 3 as a, 7 as b;",
	)
	require.NoError(t, err)

	expectation := warehouse.Records{
		warehouse.Record{"a": "0", "b": "4"},
		warehouse.Record{"a": "1", "b": "5"},
		warehouse.Record{"a": "2", "b": "6"},
		warehouse.Record{"a": "3", "b": "7"},
	}

	assert.Equal(t, expectation, rs)
}

package dal_test

import (
	"fmt"
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"

	"github.com/supasheet/dal/internal/dal"
	"github.com/supasheet/dal/internal/dbt"
	"github.com/supasheet/dal/internal/warehouse"
)

// The mock client doesn't actually query anything, the only point of it is to
// capture the SQL query that was run so that we can make assertions about it.
type mockClient struct {
	query string
}

func (mc *mockClient) Run(query string) (warehouse.Records, error) {
	mc.query = query
	return nil, nil
}

var schema = []dbt.Node{
	{
		Name: "foo", Columns: map[string]dbt.Column{
			"a": {Name: "a"},
			"b": {Name: "b"},
			"c": {Name: "c"},
		},
	},
}

func TestGenerateSql(t *testing.T) {
	type tc struct {
		name  string
		query string
		want  string
	}

	cases := []tc{
		// Basic
		{"one_field", `{foo {a}}`, `SELECT "A" FROM "FOO" LIMIT 500`},
		{"many_fields", `{foo {a b c}}`, `SELECT "A", "B", "C" FROM "FOO" LIMIT 500`},

		// Limits and offsets
		{"limit", `{foo(limit: 10) {a b c}}`, `SELECT "A", "B", "C" FROM "FOO" LIMIT 10`},
		{"offset", `{foo(offset: 10) {a b c}}`, `SELECT "A", "B", "C" FROM "FOO" LIMIT 500 OFFSET 10`},
		{"limit_offset", `{foo(limit: 10, offset: 10) {a b c}}`, `SELECT "A", "B", "C" FROM "FOO" LIMIT 10 OFFSET 10`},

		// Filters
		{"eq", `{foo(filter: {a: {eq: "z"}}) {a}}`, `SELECT "A" FROM "FOO" WHERE ("A" = 'z') LIMIT 500`},
		{"neq", `{foo(filter: {a: {neq: "z"}}) {a}}`, `SELECT "A" FROM "FOO" WHERE ("A" != 'z') LIMIT 500`},
		{"lt", `{foo(filter: {a: {lt: "z"}}) {a}}`, `SELECT "A" FROM "FOO" WHERE ("A" < 'z') LIMIT 500`},
		{"lte", `{foo(filter: {a: {lte: "z"}}) {a}}`, `SELECT "A" FROM "FOO" WHERE ("A" <= 'z') LIMIT 500`},
		{"gt", `{foo(filter: {a: {gt: "z"}}) {a}}`, `SELECT "A" FROM "FOO" WHERE ("A" > 'z') LIMIT 500`},
		{"gte", `{foo(filter: {a: {gte: "z"}}) {a}}`, `SELECT "A" FROM "FOO" WHERE ("A" >= 'z') LIMIT 500`},

		// Sort
		{"asc", `{foo(sort: {a: asc}) {a}}`, `SELECT "A" FROM "FOO" ORDER BY "A" ASC LIMIT 500`},
		{"desc", `{foo(sort: {a: desc}) {a}}`, `SELECT "A" FROM "FOO" ORDER BY "A" DESC LIMIT 500`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Build the GraphQL schema
			mc := &mockClient{}
			schema := dal.BuildSchema(mc, schema)

			// Run the query
			result := graphql.Do(graphql.Params{
				Schema:        *schema,
				RequestString: c.query,
			})

			// Inspect the captured SQL
			assert.Equal(t, c.want, mc.query)
			if c.want != mc.query {
				// Helpful to print out the result when the test fails.
				fmt.Println(result)
			}
		})
	}
}

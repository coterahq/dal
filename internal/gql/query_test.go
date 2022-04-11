package gql_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"

	"github.com/supasheet/dal/internal/dal"
	"github.com/supasheet/dal/internal/gql"
	"github.com/supasheet/dal/internal/warehouse"
)

type mockClient struct {
	queries   []string
	responses []r
}

type r warehouse.Records

func (mc *mockClient) Connect() error { return nil }

func (mc *mockClient) Run(query string) (warehouse.Records, error) {
	mc.queries = append(mc.queries, query)
	if mc.responses == nil || len(mc.responses) == 0 {
		return nil, nil
	}
	next := mc.responses[0]
	if len(mc.responses) > 1 {
		mc.responses = mc.responses[1:]
	}
	return warehouse.Records(next), nil
}

var schema = dal.Schema{
	"foo": &dal.Model{
		Name:       "foo",
		PrimaryKey: "a",
		Columns: []dal.Column{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		},
	},
	"bar": &dal.Model{
		Name:       "bar",
		PrimaryKey: "x",
		ForeignKeys: []dal.ForeignKey{
			{Model: "foo", On: "a"},
		},
		Columns: []dal.Column{
			{Name: "x"},
			{Name: "y"},
			{Name: "z"},
		},
	},
}

func TestGenerateSql(t *testing.T) {
	type tc struct {
		name      string
		query     string
		want      []string
		responses []r
	}

	cases := []tc{
		// Basic
		{
			name:  "one_field",
			query: `{foo {a}}`,
			want:  qs(`SELECT a FROM foo LIMIT 500`),
		},
		{
			name:  "many_fields",
			query: `{foo {a b c}}`,
			want:  qs(`SELECT a, b, c FROM foo LIMIT 500`),
		},

		// Limits and offsets
		{
			name:  "limit",
			query: `{foo(limit: 10) {a b c}}`,
			want:  qs(`SELECT a, b, c FROM foo LIMIT 10`),
		},
		{
			name:  "offset",
			query: `{foo(offset: 10) {a b c}}`,
			want:  qs(`SELECT a, b, c FROM foo LIMIT 500 OFFSET 10`),
		},
		{
			name:  "limit_offset",
			query: `{foo(limit: 10, offset: 10) {a b c}}`,
			want:  qs(`SELECT a, b, c FROM foo LIMIT 10 OFFSET 10`),
		},

		// Filters
		{
			name:  "eq",
			query: `{foo(filter: {a: {eq: "z"}}) {a}}`,
			want:  qs(`SELECT a FROM foo WHERE (a = 'z') LIMIT 500`),
		},
		{
			name:  "neq",
			query: `{foo(filter: {a: {neq: "z"}}) {a}}`,
			want:  qs(`SELECT a FROM foo WHERE (a != 'z') LIMIT 500`),
		},
		{
			name:  "lt",
			query: `{foo(filter: {a: {lt: "z"}}) {a}}`,
			want:  qs(`SELECT a FROM foo WHERE (a < 'z') LIMIT 500`),
		},
		{
			name:  "lte",
			query: `{foo(filter: {a: {lte: "z"}}) {a}}`,
			want:  qs(`SELECT a FROM foo WHERE (a <= 'z') LIMIT 500`),
		},
		{
			name:  "gt",
			query: `{foo(filter: {a: {gt: "z"}}) {a}}`,
			want:  qs(`SELECT a FROM foo WHERE (a > 'z') LIMIT 500`),
		},
		{
			name:  "gte",
			query: `{foo(filter: {a: {gte: "z"}}) {a}}`,
			want:  qs(`SELECT a FROM foo WHERE (a >= 'z') LIMIT 500`),
		},

		// Sort
		{
			name:  "asc",
			query: `{foo(sort: {a: asc}) {a}}`,
			want:  qs(`SELECT a FROM foo ORDER BY a ASC LIMIT 500`),
		},
		{
			name:  "desc",
			query: `{foo(sort: {a: desc}) {a}}`,
			want:  qs(`SELECT a FROM foo ORDER BY a DESC LIMIT 500`),
		},

		// Join
		{
			name:  "join",
			query: `{ bar { x foo { b c } } }`,
			want: qs(
				`SELECT x FROM bar LIMIT 500`,
				`SELECT * FROM foo WHERE (a IN (1, 2))`,
			),
			responses: []r{
				r{
					{"x": 1},
					{"x": 2},
				},
				r{
					{"a": 1, "b": 3, "c": 7},
					{"a": 1, "b": 4, "c": 8},
					{"a": 2, "b": 5, "c": 9},
					{"a": 2, "b": 6, "c": 10},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Build the GraphQL schema
			mc := &mockClient{responses: c.responses}
			schema, _ := gql.BuildSchema(mc, schema)

			// Run the query
			result := graphql.Do(graphql.Params{
				Schema:        *schema,
				RequestString: c.query,
			})

			// Inspect the captured SQL
			if !assert.Equal(t, c.want, mc.queries) {
				// Helpful to print out the result when the test fails.
				b, _ := json.MarshalIndent(result, "", "  ")
				fmt.Println("Result:")
				fmt.Println(string(b))
			}
		})
	}
}

func qs(queries ...string) []string {
	return queries
}

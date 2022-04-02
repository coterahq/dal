package gql

import (
	"fmt"
	"log"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/mitchellh/mapstructure"

	"github.com/supasheet/dal/internal/dbt"
	"github.com/supasheet/dal/internal/warehouse"
)

func init() {
	opts := goqu.DefaultDialectOptions()
	// HACK: goqu forces you to quote identifiers but we don't really want
	// that. So we use the 0 rune so that it is easy to remove later.
	opts.QuoteRune = 0
	goqu.RegisterDialect("snowflake", opts)
}

var (
	dialect = goqu.Dialect("snowflake")

	dirEnum = graphql.NewEnum(graphql.EnumConfig{
		Name:        "direction",
		Description: "Sort direction",
		Values: graphql.EnumValueConfigMap{
			"asc": &graphql.EnumValueConfig{
				Value:       "asc",
				Description: "Ascending",
			},
			"desc": &graphql.EnumValueConfig{
				Value:       "desc",
				Description: "Descending",
			},
		},
	})
)

func buildFilter(node dbt.Node) *graphql.ArgumentConfig {
	opFields := graphql.InputObjectConfigFieldMap{}
	for _, op := range []string{"eq", "neq", "lt", "gt", "lte", "gte"} {
		opFields[op] = &graphql.InputObjectFieldConfig{
			// TODO: Look up the type from the manifest and set this appropriately.
			Type: graphql.String,
		}
	}
	iocfm := graphql.InputObjectConfigFieldMap{}
	for _, col := range node.Columns {
		iocfm[col.Name] = &graphql.InputObjectFieldConfig{
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:   fmt.Sprintf("filter_%s_%s", node.Name, col.Name),
					Fields: opFields,
				},
			),
		}
	}

	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(graphql.InputObjectConfig{
			Name:   fmt.Sprintf("%s_filter", node.Name),
			Fields: iocfm,
		}),
		Description: "Filter",
	}
}

func buildSort(node dbt.Node) *graphql.ArgumentConfig {
	iocfm := graphql.InputObjectConfigFieldMap{}
	for _, col := range node.Columns {
		iocfm[col.Name] = &graphql.InputObjectFieldConfig{
			Type: dirEnum,
		}
	}

	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(graphql.InputObjectConfig{
			Name:   fmt.Sprintf("%s_sort", node.Name),
			Fields: iocfm,
		}),
		Description: "Sort",
	}
}

func parseFilter(f any) (map[string]map[string]any, error) {
	var filter map[string]map[string]any
	config := &mapstructure.DecoderConfig{
		Metadata: nil,
		Result:   &filter,
		TagName:  "json",
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		log.Printf("%v", err)
		return nil, err
	}

	err = decoder.Decode(f)
	if err != nil {
		log.Printf("%v", err)
		return nil, err
	}
	return filter, nil
}

func buildResolver(w warehouse.Client, node dbt.Node) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		// Generate the SQL query
		q := dialect.From(node.Name).Select(getSelectedFields(node.Config.Meta.Dal.PrimaryKey, p)...)

		// Handle filter
		if f, ok := p.Args["filter"]; ok {
			// Map the filter
			filter, err := parseFilter(f)
			if err != nil {
				log.Printf("%v", err)
				return warehouse.Records{}, err
			}
			// Make a goqu Ex from it
			wheres := make(goqu.Ex)
			for field, condition := range filter {
				wheres[field] = goqu.Op(condition)
			}
			q = q.Where(wheres)
		}

		// Handle sort
		if o, ok := p.Args["sort"]; ok {
			var oes []exp.OrderedExpression
			for field, dir := range o.(map[string]any) {
				c := goqu.C(field)
				var oe exp.OrderedExpression
				if dir == "asc" {
					oe = c.Asc()
				} else {
					oe = c.Desc()
				}
				oes = append(oes, oe)
			}
			q = q.Order(oes...)
		}

		// Handle the limit clause, default to 500
		limit := 500
		if l, ok := p.Args["limit"]; ok {
			limit = l.(int)
		}
		q = q.Limit(uint(limit))

		// Handle the offset
		if o, ok := p.Args["offset"]; ok {
			q = q.Offset(uint(o.(int)))
		}

		// Generate the SQL
		sql, _, err := q.ToSQL()
		if err != nil {
			log.Printf("%v", err)
			return warehouse.Records{}, err
		}

		// Run it
		// HACK: goqu forces you to quote identifiers but we don't really want that for snowflake.
		cleaned := cleanQuery(sql)
		log.Printf("Running query: %s", cleaned)
		return w.Run(cleaned)
	}
}

func cleanQuery(query string) string {
	var cleaned []rune
	for _, r := range query {
		if r != 0 {
			cleaned = append(cleaned, r)
		}
	}
	return string(cleaned)
}

// Returns the list of requested fields from the current part of the query.  It
// requires us to specify the pk to ensure that it's always present in the
// result.
func getSelectedFields(pk string, p graphql.ResolveParams) []any {
	var selectionPath []string
	for _, part := range p.Info.Path.AsArray() {
		selectionPath = append(selectionPath, part.(string))
	}

	fields := p.Info.FieldASTs

	for _, propName := range selectionPath {
		found := false

		for _, field := range fields {
			if field.Name.Value == propName {
				selections := field.SelectionSet.Selections
				fields = make([]*ast.Field, 0)

				for _, selection := range selections {
					// We only want 'raw' fields to be included in this sql
					// query. Foreign Keys are resolved differently, so we only
					// add it in if it does not have a selection set of its
					// own.
					if selection.GetSelectionSet() == nil {
						fields = append(fields, selection.(*ast.Field))
					}
				}

				found = true

				break
			}
		}

		if !found {
			return []any{}
		}
	}

	collect := []any{pk}
	for _, field := range fields {
		n := field.Name.Value
		if n != pk {
			collect = append(collect, n)
		}
	}

	return collect
}

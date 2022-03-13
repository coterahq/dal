package dal

import (
	"fmt"
	"log"
	"strings"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/mitchellh/mapstructure"

	"github.com/supasheet/dal/internal/dbt"
	"github.com/supasheet/dal/internal/warehouse"
)

var (
	// TODO goqu doesn't actually support Snowflake, but for the simple stuff
	// we're doing this works fine for now.
	dialect = goqu.Dialect("postgres")

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

// Build a GraphQL schema from a list of of dbt nodes.
func BuildSchema(w warehouse.Client, nodes []dbt.Node) *graphql.Schema {
	fields := buildRoot(w, nodes)
	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}

	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		log.Fatal(err)
	}

	return &schema
}

func buildRoot(w warehouse.Client, nodes []dbt.Node) graphql.Fields {
	fields := make(graphql.Fields)
	for _, node := range nodes {
		fields[node.Name] = &graphql.Field{
			Description: node.Description,
			Type:        graphql.NewList(buildType(node)),
			Resolve:     buildResolver(w, node),
			Args: graphql.FieldConfigArgument{
				"limit": &graphql.ArgumentConfig{
					Type:        graphql.Int,
					Description: "Limit",
				},
				"offset": &graphql.ArgumentConfig{
					Type:        graphql.Int,
					Description: "Offset",
				},
				"filter": buildFilter(node),
				"sort":   buildSort(node),
			},
		}
	}
	return fields
}

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

func buildType(node dbt.Node) *graphql.Object {
	fields := make(graphql.Fields)
	for _, col := range node.Columns {
		fields[col.Name] = &graphql.Field{
			Type:        graphql.String,
			Description: col.Description,
		}
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:   node.Name,
		Fields: fields,
	})
}

//type Filter struct {
//	Op  string      `json:"op"`
//	Val interface{} `json:"val"`
//}
//
//func (f Filter) Fragment() goqu.Ex {
//	return goqu.Ex{
//		f.Field: goqu.Op{f.Op: f.Val},
//	}
//}

func parseFilter(f interface{}) (map[string]map[string]interface{}, error) {
	var filter map[string]map[string]interface{}
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
	return func(p graphql.ResolveParams) (interface{}, error) {
		// Generate the SQL query
		// TODO The uppercase name is snowflake specific and doesn't handle
		// cases where the table was created with a quoted name.
		q := dialect.From(strings.ToUpper(node.Name)).Select(getSelectedFields(p)...)

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
				wheres[strings.ToUpper(field)] = goqu.Op(condition)
			}
			q = q.Where(wheres)
		}

		// Handle sort
		if o, ok := p.Args["sort"]; ok {
			var oes []exp.OrderedExpression
			for field, dir := range o.(map[string]interface{}) {
				c := goqu.C(strings.ToUpper(field))
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
		log.Printf("Running query: %s", sql)
		return w.Run(sql)
	}
}

// Returns the list of request fields listed under provider selection path in the Graphql query.
func getSelectedFields(p graphql.ResolveParams) []interface{} {
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
					fields = append(fields, selection.(*ast.Field))
				}

				found = true

				break
			}
		}

		if !found {
			return []interface{}{}
		}
	}

	var collect []interface{}

	for _, field := range fields {
		// TODO the uppercasing is snowflake specific and technically not even
		// correct depending on how the the schema was created.
		collect = append(collect, strings.ToUpper(field.Name.Value))
	}

	return collect
}

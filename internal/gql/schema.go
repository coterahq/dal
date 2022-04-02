package gql

import (
	"fmt"

	"github.com/graph-gophers/dataloader"
	"github.com/graphql-go/graphql"

	"github.com/supasheet/dal/internal/dbt"
	"github.com/supasheet/dal/internal/warehouse"
)

func BuildSchema(wc warehouse.Client, nodes []dbt.Node) (*graphql.Schema, error) {
	sb := &schemaBuilder{
		nodes:   nodes,
		wc:      wc,
		types:   make(map[string]*graphql.Object),
		loaders: make(map[string]*dataloader.Loader),
	}
	return sb.build()
}

type schemaBuilder struct {
	nodes   []dbt.Node
	wc      warehouse.Client
	types   map[string]*graphql.Object
	loaders map[string]*dataloader.Loader
}

// This builds the graphql schema.
func (sb *schemaBuilder) build() (*graphql.Schema, error) {
	sb.resolveTypes()

	fields := make(graphql.Fields)
	for _, node := range sb.nodes {
		fields[node.Name] = &graphql.Field{
			Description: node.Description,
			Type:        graphql.NewList(sb.types[node.Name]),
			Resolve:     buildResolver(sb.wc, node),
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
	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}

	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		return nil, err
	}

	return &schema, nil
}

// This is a first pass through the list of dbt nodes. It creates simple types
// with no foreign key information.
func (sb *schemaBuilder) resolveTypes() {
	for _, node := range sb.nodes {
		// For each node we simply look at all of the columns in the manifest,
		// and create a field for each one.
		fields := make(graphql.Fields)
		for _, col := range node.Columns {
			fields[col.Name] = &graphql.Field{
				// TODO this currently says every field is a string, which is wrong.
				// We can leverage the dbt catalog to get the information.
				Type: graphql.String,
				// Bring through the description from the dbt docs.
				Description: col.Description,
			}
		}

		sb.types[node.Name] = graphql.NewObject(graphql.ObjectConfig{
			Name:   node.Name,
			Fields: fields,
		})
	}

	// Now we've completed the first pass through the types, we should go
	// through and add in the foreign keys.
	sb.resolveForeignKeys()
}

// This must be called after resolving the types and loaders. It will make a
// final pass through the nodes, and look for any foreign keys that need to be
// added to the types. It will create they appropriate fields on the types and
// then leverage the loaders to create resolvers for them.
func (sb *schemaBuilder) resolveForeignKeys() {
	// For each node in the list
	for _, node := range sb.nodes {
		// Get a reference to the _actual item_
		node := node
		// Get the type we built
		t := sb.types[node.Name]
		// Then through all of it's foreign keys
		for _, fk := range node.Config.Meta.Dal.ForeignKeys {
			// Get the type for the related model
			rel := sb.types[fk.Model]

			// For each one we create a loader that filters the target
			// according to the join key
			loader := buildOneToManyLoader(sb.wc, fk.Model, fk.RightOn)

			// And then we add a field for the relationship.
			t.AddFieldConfig(fk.Model, &graphql.Field{
				Type:        graphql.NewList(rel),
				Description: fmt.Sprintf("Associated %s", fk.Model),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					source := p.Source.(warehouse.Record)
					rawKey := source[node.Config.Meta.Dal.PrimaryKey]
					thunk := loader.Load(p.Context, NewResolverKey(rawKey))
					return func() (any, error) {
						return thunk()
					}, nil
				},
			})
		}
	}
}

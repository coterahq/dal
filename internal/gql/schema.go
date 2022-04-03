package gql

import (
	"fmt"

	"github.com/graph-gophers/dataloader"
	"github.com/graphql-go/graphql"

	"github.com/supasheet/dal/internal/dal"
	"github.com/supasheet/dal/internal/warehouse"
)

func BuildSchema(wc warehouse.Client, s dal.Schema) (*graphql.Schema, error) {
	sb := &schemaBuilder{
		schema:  s,
		wc:      wc,
		types:   make(map[string]*graphql.Object),
		loaders: make(map[string]*dataloader.Loader),
	}
	return sb.build()
}

type schemaBuilder struct {
	schema  dal.Schema
	wc      warehouse.Client
	types   map[string]*graphql.Object
	loaders map[string]*dataloader.Loader
}

// This builds the graphql schema.
func (sb *schemaBuilder) build() (*graphql.Schema, error) {
	sb.resolveTypes()

	fields := make(graphql.Fields)
	for name, model := range sb.schema {
		fields[name] = &graphql.Field{
			Description: model.Description,
			Type:        graphql.NewList(sb.types[name]),
			Resolve:     buildResolver(sb.wc, model),
			Args: graphql.FieldConfigArgument{
				"limit": &graphql.ArgumentConfig{
					Type:        graphql.Int,
					Description: "Limit",
				},
				"offset": &graphql.ArgumentConfig{
					Type:        graphql.Int,
					Description: "Offset",
				},
				"filter": buildFilter(model),
				"sort":   buildSort(model),
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

// This is a first pass through the models. It creates simple types
// with no foreign key information.
func (sb *schemaBuilder) resolveTypes() {
	for name, model := range sb.schema {
		model := model
		// For each node we simply look at all of the columns in the manifest,
		// and create a field for each one.
		fields := make(graphql.Fields)
		for _, col := range model.Columns {
			fields[col.Name] = &graphql.Field{
				// Map the dal type to the appropriate GrahpQL type.
				Type: mapScalarType(col.Type),
				// Bring through the description from the model.
				Description: col.Description,
			}
		}

		sb.types[name] = graphql.NewObject(graphql.ObjectConfig{
			Name:        name,
			Description: model.Description,
			Fields:      fields,
		})
	}

	// Now we've completed the first pass through the types, we should go
	// through and add in the foreign keys.
	sb.resolveForeignKeys()
}

func mapScalarType(ds dal.Scalar) *graphql.Scalar {
	switch ds {
	case dal.ID:
		return graphql.ID
	case dal.Int:
		return graphql.Int
	case dal.Float:
		return graphql.Float
	case dal.Boolean:
		return graphql.Boolean
	case dal.String:
		return graphql.String
	case dal.DateTime:
		return graphql.DateTime
	default:
		return graphql.String
	}
}

// This must be called after resolving the types and loaders. It will make a
// final pass through the nodes, and look for any foreign keys that need to be
// added to the types. It will create they appropriate fields on the types and
// then leverage the loaders to create resolvers for them.
func (sb *schemaBuilder) resolveForeignKeys() {
	// For each node in the list
	for name, model := range sb.schema {
		// Get a reference to the _actual item_
		model := model
		// Get the type we built
		t := sb.types[name]
		// Then through all of it's foreign keys
		for _, fk := range model.ForeignKeys {
			// Get the type for the related model
			rel := sb.types[fk.Model]

			// For each one we create a loader that filters the target
			// according to the join key
			loader := buildOneToManyLoader(sb.wc, fk.Model, fk.On)

			// And then we add a field for the relationship.
			t.AddFieldConfig(fk.Model, &graphql.Field{
				Type:        graphql.NewList(rel),
				Description: fmt.Sprintf("Associated %s", fk.Model),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					source := p.Source.(warehouse.Record)
					rawKey := source[model.PrimaryKey]
					thunk := loader.Load(p.Context, NewResolverKey(rawKey))
					return func() (any, error) {
						return thunk()
					}, nil
				},
			})
		}
	}
}

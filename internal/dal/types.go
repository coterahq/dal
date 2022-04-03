package dal

import (
	"errors"
	"fmt"
)

// The type system for dal is pretty straight forward.
// It consists of the following scalar types which are built into GraphQL:
// - ID
// - Int
// - Float
// - String
// - Boolean
// - DateTime (in RFC3339 format)
type Scalar string

const (
	ID       Scalar = "ID"
	Int      Scalar = "Int"
	Float    Scalar = "Float"
	Boolean  Scalar = "Boolean"
	String   Scalar = "String"
	DateTime Scalar = "DateTime"
)

/**
There are two aspect to the type system.
The first is the type metadata for the schema.
The second is the various types at run time.

1. For this we need to parse the catalog file, and extract the types for each
column. For each of those types, which are database specific, we need to map
them into our type system. This then needs to feed through to the schema builder.

2. For this, we need to know which type we're meant to get for a field, and
then we need to coerce it to be of the correct runtime value.
*/

var (
	ErrNoSuchModel = errors.New("no such model")
)

type Schema map[string]*Model

func (s Schema) AddModel(name, description, pk string) *Model {
	m := &Model{Name: name, PrimaryKey: pk, schema: s}
	s[name] = m
	return m
}

// This represents a dal model. It's essentially a type that describes any flat
// data structure, for example a table. It also encodes information about how
// the models relate to each other.
type Model struct {
	Name        string
	PrimaryKey  string
	Description string
	Columns     []Column
	ForeignKeys []ForeignKey

	schema Schema
}

// Adds a new column to the model.
func (m *Model) AddColumn(name, description string, t Scalar) {
	m.Columns = append(m.Columns, Column{Name: name, Description: description, Type: t})
}

// Adds a new foreign key to the model. It requires that the model already
// exists in the Schema.
func (m *Model) AddForeignKey(model, on string) error {
	if _, ok := m.schema[model]; ok {
		m.ForeignKeys = append(m.ForeignKeys, ForeignKey{Model: model, On: on})
		return nil
	}
	return fmt.Errorf("cannot create foreign key: %s is not a valid model: %w", model, ErrNoSuchModel)
}

type Column struct {
	Name        string
	Description string
	Type        Scalar
}

type ForeignKey struct {
	Model string
	On    string
}

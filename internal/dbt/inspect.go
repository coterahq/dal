package dbt

import (
	"errors"
	"fmt"

	"github.com/supasheet/dal/internal/dal"
	"github.com/supasheet/dal/internal/warehouse"
)

// Inspects a dbt project and builds a dal schema and a warehouse client.
func Inspect() (dal.Schema, warehouse.Client, error) {
	project := LoadProject()
	profile := project.LoadProfile()
	// Just load the default target
	target := profile.Outputs[profile.Target]

	// First let's setup the warehouse connection
	var client warehouse.Client
	if target.Type == "snowflake" {
		client = warehouse.NewSnowflake(
			warehouse.SnowflakeCredentials{
				AccountId: target.Account,
				User:      target.User,
				Password:  target.Password,
				Database:  target.Database,
				Schema:    target.Schema,
				Warehouse: target.Warehouse,
			},
		)
	} else {
		// We don't support that kind of warehouse, so return an error.
		return nil, nil, errors.New(fmt.Sprintf("warehouse type %s is not supported", target.Type))
	}

	// Now we can load up the manifest and try to build a dal schema from it.
	nodes := LoadManifestNodes()
	catalog := LoadCatalog()
	schema := make(dal.Schema)

	// First up create all of the nodes
	for _, node := range nodes {
		// Add the model
		model := schema.AddModel(node.Name, node.Description, node.Config.Meta.Dal.PrimaryKey)
		for _, col := range node.Columns {
			// Before creating the column we need to look up the appropriate
			// type for it from the schema.
			colType, err := catalog.typeOfColumn(node.UniqueID, col.Name)
			if err != nil {
				return nil, nil, err
			}
			model.AddColumn(col.Name, col.Description, client.MapType(colType))
		}
	}

	// Then go through and make all the foreign keys
	for _, node := range nodes {
		node := node
		model := schema[node.Name]
		for _, fk := range node.Config.Meta.Dal.ForeignKeys {
			if err := model.AddForeignKey(fk.Model, fk.RightOn); err != nil {
				return nil, nil, err
			}
		}
	}

	return schema, client, nil
}

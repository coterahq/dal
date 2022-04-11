package dbt

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

func LoadCatalog() *Catalog {
	f, err := os.Open(filepath.Join("target", "catalog.json"))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var c Catalog
	err = yaml.NewDecoder(f).Decode(&c)
	if err != nil {
		log.Fatal(err)
	}

	return &c
}

// v1 DBT Catalog https://schemas.getdbt.com/dbt/catalog/v1.json
type Catalog struct {
	Metadata CatalogMetadata        `json:"metadata"`
	Nodes    map[string]CatalogNode `json:"nodes"`
}

func (c *Catalog) typeOfColumn(uniqueId, column string) (string, error) {
	for key, node := range c.Nodes {
		if key == uniqueId {
			for name, col := range node.Columns {
				if strings.ToLower(name) == strings.ToLower(column) {
					return col.Type, nil
				}
			}
			return "", fmt.Errorf("column %s not found on model %s", column, uniqueId)
		}
	}
	return "", fmt.Errorf("model %s not in dbt catalog", uniqueId)
}

type CatalogMetadata struct {
	DbtSchemaVersion string    `json:"dbt_schema_version"`
	DbtVersion       string    `json:"dbt_version"`
	GeneratedAt      time.Time `json:"generated_at"`
	InvocationID     string    `json:"invocation_id"`
	Env              struct {
	} `json:"env"`
}

type CatalogNode struct {
	Metadata CatalogNodeMetadata           `json:"metadata"`
	Columns  map[string]CatalogNodeColumns `json:"columns"`
	Stats    map[string]CatalogNodeStats   `json:"stats"`
	UniqueID string                        `json:"unique_id"`
}

type CatalogNodeMetadata struct {
	Type     string      `json:"type"`
	Schema   string      `json:"schema"`
	Name     string      `json:"name"`
	Database string      `json:"database"`
	Comment  interface{} `json:"comment"`
	Owner    string      `json:"owner"`
}

type CatalogNodeColumns struct {
	Type    string      `json:"type"`
	Index   int         `json:"index"`
	Name    string      `json:"name"`
	Comment interface{} `json:"comment"`
}

type CatalogNodeStats struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Value       string `json:"value"`
	Include     bool   `json:"include"`
	Description string `json:"description"`
}

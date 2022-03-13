package dbt

import (
	"encoding/json"
	"log"
	"os"

	"github.com/mitchellh/mapstructure"
)

func Manifest() []Node {
	// Look for the manifest in the default location
	f, err := os.Open("./target/manifest.json")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Decode the manifest
	var dbtManifest map[string]interface{}
	err = json.NewDecoder(f).Decode(&dbtManifest)
	if err != nil {
		log.Fatal(err)
	}

	// Get the nodes off of the manifest, that's all we care about.
	nodes, ok := dbtManifest["nodes"]
	if !ok {
		log.Fatal("invalid dbt manifest")
	}

	// Look through all the nodes, we're only interested in models which have
	// been configured for dal to expose.
	var exposed []Node
	for _, n := range nodes.(map[string]interface{}) {
		var node Node
		config := &mapstructure.DecoderConfig{
			Metadata: nil,
			Result:   &node,
			TagName:  "json",
		}
		decoder, err := mapstructure.NewDecoder(config)
		if err != nil {
			log.Fatal(err)
		}

		err = decoder.Decode(n)
		if err != nil {
			log.Fatal(err)
		}

		if node.ResourceType == "model" && node.Config.Meta.Dal.Expose == true {
			exposed = append(exposed, node)
			continue
		}
	}

	return exposed
}

type Node struct {
	RawSql       string `json:"raw_sql"`
	Compiled     bool   `json:"compiled"`
	ResourceType string `json:"resource_type"`
	DependsOn    struct {
		Macros []interface{} `json:"macros"`
		Nodes  []string      `json:"nodes"`
	} `json:"depends_on"`
	Config           NodeConfig `json:"config"`
	Database         string     `json:"database"`
	Schema           string     `json:"schema"`
	Fqn              []string   `json:"fqn"`
	UniqueID         string     `json:"unique_id"`
	PackageName      string     `json:"package_name"`
	RootPath         string     `json:"root_path"`
	Path             string     `json:"path"`
	OriginalFilePath string     `json:"original_file_path"`
	Name             string     `json:"name"`
	Alias            string     `json:"alias"`
	Checksum         struct {
		Name     string `json:"name"`
		Checksum string `json:"checksum"`
	} `json:"checksum"`
	Tags        []interface{}     `json:"tags"`
	Refs        [][]string        `json:"refs"`
	Sources     []interface{}     `json:"sources"`
	Description string            `json:"description"`
	Columns     map[string]Column `json:"columns"`
	Meta        struct {
	} `json:"meta"`
	Docs struct {
		Show bool `json:"show"`
	} `json:"docs"`
	PatchPath        string      `json:"patch_path"`
	CompiledPath     string      `json:"compiled_path"`
	BuildPath        interface{} `json:"build_path"`
	Deferred         bool        `json:"deferred"`
	UnrenderedConfig struct {
		Materialized string `json:"materialized"`
	} `json:"unrendered_config"`
	CreatedAt         float64       `json:"created_at"`
	CompiledSql       string        `json:"compiled_sql"`
	ExtraCtesInjected bool          `json:"extra_ctes_injected"`
	ExtraCtes         []interface{} `json:"extra_ctes"`
	RelationName      string        `json:"relation_name"`
}

type NodeConfig struct {
	Enabled  bool          `json:"enabled"`
	Alias    interface{}   `json:"alias"`
	Schema   interface{}   `json:"schema"`
	Database interface{}   `json:"database"`
	Tags     []interface{} `json:"tags"`
	Meta     struct {
		Dal DalNodeConfig `json:"dal"`
	} `json:"meta"`
	Materialized string `json:"materialized"`
	PersistDocs  struct {
	} `json:"persist_docs"`
	Quoting struct {
	} `json:"quoting"`
	ColumnTypes struct {
	} `json:"column_types"`
	FullRefresh    interface{}   `json:"full_refresh"`
	OnSchemaChange string        `json:"on_schema_change"`
	PostHook       []interface{} `json:"post-hook"`
	PreHook        []interface{} `json:"pre-hook"`
}

type DalNodeConfig struct {
	Expose bool `json:"expose"`
}

type Column struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Meta        struct {
	} `json:"meta"`
	DataType interface{}   `json:"data_type"`
	Quote    interface{}   `json:"quote"`
	Tags     []interface{} `json:"tags"`
}

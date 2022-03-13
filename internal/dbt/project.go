package dbt

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type ProjectConfig struct {
	Name              string                 `json:"name"`
	ConfigVersion     int                    `json:"config-version"`
	Version           string                 `json:"version"`
	Profile           string                 `json:"profile"`
	ModelPaths        []string               `json:"model-paths"`
	SeedPaths         []string               `json:"seed-paths"`
	TestPaths         []string               `json:"test-paths"`
	AnalysisPaths     []string               `json:"analysis-paths"`
	MacroPaths        []string               `json:"macro-paths"`
	TargetPath        string                 `json:"target-path"`
	CleanTargets      []string               `json:"clean-targets"`
	RequireDbtVersion []string               `json:"require-dbt-version"`
	Models            map[string]ModelConfig `json:"models"`
}

type ModelConfig struct {
	Materialized string `json:"materialized"`
	Staging      struct {
		Materialized string `json:"materialized"`
	} `json:"staging"`
}

func Project() ProjectConfig {
	f, err := os.Open("./dbt_project.yml")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var pc ProjectConfig
	err = yaml.NewDecoder(f).Decode(&pc)
	if err != nil {
		log.Fatal(err)
	}

	return pc
}

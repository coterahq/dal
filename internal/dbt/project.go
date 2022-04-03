package dbt

import (
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Project struct {
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

func LoadProject() *Project {
	f, err := os.Open("./dbt_project.yml")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var pc Project
	err = yaml.NewDecoder(f).Decode(&pc)
	if err != nil {
		log.Fatal(err)
	}

	return &pc
}

func (p *Project) LoadProfile() Profile {
	// Find the home directory
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}

	// Open the profile file
	f, err := os.Open(filepath.Join(home, ".dbt", "profiles.yml"))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Load the profiles
	var profiles Profiles
	err = yaml.NewDecoder(f).Decode(&profiles)
	if err != nil {
		log.Fatal(err)
	}

	// Return the selected profile
	return profiles[p.Profile]
}

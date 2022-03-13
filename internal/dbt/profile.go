package dbt

import (
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type DbtProfiles map[string]DbtProfile

type DbtProfile struct {
	Target  string            `json:"target"`
	Outputs map[string]Output `json:"outputs"`
}

// TODO: half of this is snowflake specific but that's fine for now
type Output struct {
	Type                   string `json:"type"`
	Account                string `json:"account"`
	User                   string `json:"user"`
	Password               string `json:"password"`
	Role                   string `json:"role"`
	Database               string `json:"database"`
	Warehouse              string `json:"warehouse"`
	Schema                 string `json:"schema"`
	Threads                int    `json:"threads"`
	ClientSessionKeepAlive bool   `json:"client_session_keep_alive"`
	QueryTag               string `json:"query_tag"`
	ConnectRetries         int    `json:"connect_retries"`
	ConnectTimeout         int    `json:"connect_timeout"`
	RetryOnDatabaseErrors  bool   `json:"retry_on_database_errors"`
	RetryAll               bool   `json:"retry_all"`
}

func Profile(profile string) DbtProfile {
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
	var profiles DbtProfiles
	err = yaml.NewDecoder(f).Decode(&profiles)
	if err != nil {
		log.Fatal(err)
	}

	// Return the selected profile
	return profiles[profile]
}

package main

import (
	"os"

	"github.com/supasheet/dal/cmd"
	"github.com/supasheet/dal/internal/dbt"
	"github.com/supasheet/dal/internal/warehouse"
)

func main() {
	// First lets open the dbt project config
	config := dbt.Project()

	// Now lets look up the specified warehouse profile. We'll just use
	// the default target for now.
	profile := dbt.Profile(config.Profile)
	creds := profile.Outputs[profile.Target]

	w := warehouse.NewSnowflake()
	w.Connect(warehouse.SnowflakeCredentials{
		AccountId: creds.Account,
		User:      creds.User,
		Password:  creds.Password,
		Database:  creds.Database,
		Schema:    creds.Schema,
		Warehouse: creds.Warehouse,
	})
	cli := cmd.NewCli(w)
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}

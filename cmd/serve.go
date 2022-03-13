package cmd

import (
	"log"

	"github.com/spf13/cobra"
	"github.com/supasheet/dal/internal/dal"
	"github.com/supasheet/dal/internal/dbt"
	"github.com/supasheet/dal/internal/warehouse"
)

func serveCmd(w warehouse.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Serve your dal api",
		Long:  "Starts a graphql server that allows you to programatically access dbt models.",
		Run: func(cmd *cobra.Command, args []string) {
			// Inspect the manifest and build a schema
			nodes := dbt.Manifest()
			schema := dal.BuildSchema(w, nodes)
			log.Print("Starting dal server on port 8080")
			log.Print("GraphiQL available at http://localhost:8080/graphql")
			dal.Serve(schema)
		},
	}
}

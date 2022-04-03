package cmd

import (
	"log"

	"github.com/spf13/cobra"
	"github.com/supasheet/dal/internal/dbt"
	"github.com/supasheet/dal/internal/gql"
)

func serveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Serve your dal api",
		Long:  "Starts a graphql server that allows you to programatically access dbt models.",
		Run: func(cmd *cobra.Command, args []string) {
			// Inspect the manifest and build a schema
			dalSchema, client, err := dbt.Inspect()
			if err != nil {
				log.Fatalf("ERROR loading dbt project: %v", err)
			}

			gqlSchema, err := gql.BuildSchema(client, dalSchema)
			if err != nil {
				log.Fatalf("ERROR creating schema: %v", err)
			}

			err = client.Connect()
			if err != nil {
				log.Fatalf("ERROR failed to connect to data warehouse: %v", err)
			}

			log.Print("Starting dal server on port 8080")
			log.Print("GraphiQL available at http://localhost:8080/graphql")
			gql.Serve(gqlSchema)
		},
	}
}

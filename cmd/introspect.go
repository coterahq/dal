package cmd

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/graphql-go/graphql"
	"github.com/spf13/cobra"

	"github.com/supasheet/dal/internal/dbt"
	"github.com/supasheet/dal/internal/gql"
)

func introspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "introspect",
		Short: "Introspect your dal api schema",
		Long:  "Introspects and prints the GraphQL schema for your dbt project.",
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

			result := graphql.Do(graphql.Params{
				Schema:        *gqlSchema,
				RequestString: introspectionQuery,
			})

			if result.HasErrors() {
				log.Fatalf("ERROR introspecting schema: %v", result.Errors)
				return
			} else {
				b, err := json.MarshalIndent(result, "", "  ")
				if err != nil {
					log.Fatalf("ERROR: %v", err)
				}
				fmt.Println(string(b))
			}
		},
	}
}

var introspectionQuery = `
  query IntrospectionQuery {
    __schema {
      queryType { name }
      mutationType { name }
      subscriptionType { name }
      types {
        ...FullType
      }
      directives {
        name
        description
		locations
        args {
          ...InputValue
        }
        # deprecated, but included for coverage till removed
		onOperation
        onFragment
        onField
      }
    }
  }
  fragment FullType on __Type {
    kind
    name
    description
    fields(includeDeprecated: true) {
      name
      description
      args {
        ...InputValue
      }
      type {
        ...TypeRef
      }
      isDeprecated
      deprecationReason
    }
    inputFields {
      ...InputValue
    }
    interfaces {
      ...TypeRef
    }
    enumValues(includeDeprecated: true) {
      name
      description
      isDeprecated
      deprecationReason
    }
    possibleTypes {
      ...TypeRef
    }
  }
  fragment InputValue on __InputValue {
    name
    description
    type { ...TypeRef }
    defaultValue
  }
  fragment TypeRef on __Type {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
      }
    }
  }
`

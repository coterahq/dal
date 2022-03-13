package dal

import (
	"log"
	"net/http"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
)

func Serve(schema *graphql.Schema) {
	// Create a new HTTP handler
	h := handler.New(&handler.Config{
		Schema: schema,
		// Pretty print JSON response
		Pretty: true,
		// Host a GraphiQL Playground to use for testing Queries
		GraphiQL:   true,
		Playground: false,
	})

	http.Handle("/graphql", h)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

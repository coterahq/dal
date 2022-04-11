package gql

import (
	"context"
	"log"

	"github.com/doug-martin/goqu/v9"
	"github.com/graph-gophers/dataloader"
	"github.com/supasheet/dal/internal/warehouse"
)

func buildOneToManyLoader(w warehouse.Client, table, joinKey string) *dataloader.Loader {
	batchFn := func(ctx context.Context, keys dataloader.Keys) []*dataloader.Result {
		// First we need to get the list of ids to run the query with.
		var ids []any
		// TODO using Raw like this could possibly blow stuff up. Need to
		// enumerate and deal with all types properly!
		for _, k := range keys {
			ids = append(ids, k.Raw())
		}

		// Now we can run the query.
		rs, err := queryByIds(w, table, joinKey, ids)
		if err != nil {
			return dataloadErr(err)
		}

		// Now we can group by the primary keys
		byKey := make(map[any][]any)
		for _, r := range rs {
			byKey[r[joinKey]] = append(byKey[r[joinKey]], r)
		}

		// Then we can build the dataloader results.
		var results []*dataloader.Result
		for _, id := range ids {
			// For each of the requested ids we _must_ return a result, and the
			// default is no results, so nil.
			result := dataloader.Result{
				Data:  nil,
				Error: nil,
			}
			// However, if we actually got some data for this key, we should
			// return that.
			if records, ok := byKey[id]; ok {
				result.Data = records
			}
			// Add the results back into the result array.
			results = append(results, &result)
		}
		return results
	}
	return dataloader.NewBatchedLoader(batchFn)
}

// Currently this just fetches all of the columns. Not ideal but it's also not
// trivial to get the selected columns into the data loader. It's also very
// cache friendly. Would have to write a custom non-compliant dataloader to
// select just the required fields.
func queryByIds(w warehouse.Client, table, key string, ids []any) (warehouse.Records, error) {
	q := dialect.From(table).Select(goqu.Star()).Where(goqu.Ex{key: ids})

	// Generate the SQL
	sql, _, err := q.ToSQL()
	if err != nil {
		log.Printf("%v", err)
		return warehouse.Records{}, err
	}

	// Run it
	cleaned := cleanQuery(sql)
	log.Printf("Running query: %s", cleaned)
	return w.Run(cleaned)
}

func dataloadErr(err error) []*dataloader.Result {
	var results []*dataloader.Result
	var result dataloader.Result
	result.Error = err
	results = append(results, &result)
	return results
}

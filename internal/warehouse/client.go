package warehouse

import (
	"database/sql"
	"strings"
)

type Client interface {
	// TODO this is a convenient but not ideal interface. It pulls all the
	// results back into memory and it's better to stream!
	Run(string) (Records, error)
}

type (
	Record  map[string]any
	Records []Record
)

func rowsToRecords(rs *sql.Rows) (Records, error) {
	// Get the columns from the result set
	cols, err := rs.Columns()
	if err != nil {
		return nil, err
	}

	// We need a buffer to store values as we construct the result map. We
	// also need an array of pointers to each slot in that buffer, as
	// that's how Scan works.
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i, _ := range vals {
		ptrs[i] = &vals[i]
	}

	// Extract the results
	records := []Record{}
	for rs.Next() {
		// Scan the values
		if err := rs.Scan(ptrs...); err != nil {
			return nil, err
		}
		// Ok, we're ready to build the map for this record
		record := make(Record)
		for i, c := range cols {
			// TODO: Lower casing the identifier names should probably not be
			// handled here. The gql mapping layer is the bit htat cares about
			// this and should force everything to be lowercase. Then again,
			// it's an 'efficient' place to do it.
			record[strings.ToLower(c)] = vals[i]
		}
		records = append(records, record)
	}
	return records, nil
}

package warehouse

import (
	"database/sql"
	"regexp"
	"strings"

	"github.com/supasheet/dal/internal/dal"
)

type Client interface {
	// Initialises the data warehosue connection.
	Connect() error

	// TODO this is a convenient but not ideal interface. It pulls all the
	// results back into memory and it's better to stream!
	Run(string) (Records, error)

	// Maps a type from the data warehouse's type system to the appropriate dal
	// type.
	MapType(string) dal.Scalar
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
	// TODO: Lower casing the identifier names should probably not be
	// handled here. The gql mapping layer is the bit htat cares about
	// this and should force everything to be lowercase. Then again,
	// it's an 'efficient' place to do it.
	for i := 0; i < len(cols); i++ {
		cols[i] = strings.ToLower(cols[i])
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
			record[c] = vals[i]
		}
		records = append(records, record)
	}
	return records, nil
}

type dataTypeMatcher struct {
	id       *regexp.Regexp
	int      *regexp.Regexp
	float    *regexp.Regexp
	boolean  *regexp.Regexp
	string   *regexp.Regexp
	dateTime *regexp.Regexp
}

func (dtm *dataTypeMatcher) match(raw string) dal.Scalar {
	switch {
	case dtm.id.MatchString(raw):
		return dal.ID
	case dtm.int.MatchString(raw):
		return dal.Int
	case dtm.float.MatchString(raw):
		return dal.Float
	case dtm.boolean.MatchString(raw):
		return dal.Boolean
	case dtm.string.MatchString(raw):
		return dal.String
	case dtm.dateTime.MatchString(raw):
		return dal.DateTime
	default:
		return dal.String
	}
}

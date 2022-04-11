package dbt

type Profiles map[string]Profile

type Profile struct {
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

module github.com/hibiken/asynq

go 1.19

require (
	github.com/go-redis/redis/v8 v8.11.2
	github.com/golang/protobuf v1.4.2
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.2.0
	github.com/mattn/go-sqlite3 v0.0.0-00010101000000-000000000000
	github.com/robfig/cron/v3 v3.0.1
	github.com/rqlite/gorqlite v0.0.0-20230310040812-ec5e524a562e
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	go.uber.org/goleak v1.1.0
	go.uber.org/multierr v1.7.0
	golang.org/x/sys v0.0.0-20210112080510-489259a85091
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/protobuf v1.25.0
)

require (
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/lint v0.0.0-20190930215403-16217165b5de // indirect
	golang.org/x/tools v0.0.0-20201224043029-2b0845dc783e // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace (
	github.com/mattn/go-sqlite3 => github.com/rqlite/go-sqlite3 v1.28.0
	github.com/rqlite/gorqlite => github.com/eluv-io/gorqlite v0.0.9
)

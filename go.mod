module github.com/hibiken/asynq

go 1.18

require (
	github.com/go-redis/redis/v8 v8.11.2
	github.com/golang/protobuf v1.4.2
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.2.0
	github.com/mattn/go-sqlite3 v0.0.0-00010101000000-000000000000
	github.com/robfig/cron/v3 v3.0.1
	github.com/rqlite/gorqlite v0.0.0-20210804113434-b4935d2eab04
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/goleak v1.1.0
	go.uber.org/multierr v1.7.0
	golang.org/x/sys v0.0.0-20210112080510-489259a85091
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/protobuf v1.25.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

replace (
	github.com/mattn/go-sqlite3 => github.com/rqlite/go-sqlite3 v1.27.1
	github.com/rqlite/gorqlite => github.com/eluv-io/gorqlite v0.0.7-0.20230505172606-a5c47a7486e2
)

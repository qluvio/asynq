module github.com/hibiken/asynq

go 1.13

require (
	github.com/go-redis/redis/v8 v8.11.2
	github.com/golang/protobuf v1.4.2
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.2.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/rqlite/gorqlite v0.0.0-20210804113434-b4935d2eab04
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.6.1
	go.uber.org/goleak v1.1.0
	golang.org/x/sys v0.0.0-20210112080510-489259a85091
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/protobuf v1.25.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

replace github.com/rqlite/gorqlite => github.com/eluv-io/gorqlite v0.0.1

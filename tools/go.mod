module github.com/hibiken/asynq/tools

go 1.13

require (
	github.com/fatih/color v1.9.0
	github.com/go-redis/redis/v8 v8.11.2
	github.com/hibiken/asynq v0.19.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.0
)

replace (
	github.com/hibiken/asynq => ./..
	github.com/mattn/go-sqlite3 => github.com/rqlite/go-sqlite3 v1.27.1
	github.com/rqlite/gorqlite => github.com/eluv-io/gorqlite v0.0.6
)

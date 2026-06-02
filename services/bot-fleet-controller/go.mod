module github.com/iicpc/bot-fleet-controller

go 1.25.0

require (
	github.com/go-chi/chi/v5 v5.3.0
	github.com/iicpc/libs v0.0.0
	github.com/iicpc/schemas v0.0.0
	github.com/jackc/pgx/v5 v5.9.2
	github.com/segmentio/kafka-go v0.4.51
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/net v0.54.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.37.0 // indirect
)

replace github.com/iicpc/libs => ../../libs/go

replace github.com/iicpc/schemas => ../../schemas/go

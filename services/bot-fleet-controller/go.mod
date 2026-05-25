module github.com/iicpc/bot-fleet-controller

go 1.25.0

require (
	github.com/go-chi/chi/v5 v5.1.0
	github.com/iicpc/libs v0.0.0
	github.com/iicpc/schemas v0.0.0
	github.com/jackc/pgx/v5 v5.6.0
	github.com/segmentio/kafka-go v0.4.47
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/iicpc/libs => ../../libs/go

replace github.com/iicpc/schemas => ../../schemas/go

install:
		cd cmd/pgslice && go install

test:
		cd cmd/pgslice && go test

lint:
		golint cmd/pgslice internal/app/pgslice

release:
		goreleaser

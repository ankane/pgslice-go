install:
	cd cmd/pgslice && go install

test:
	cd cmd/pgslice && go test

lint:
	golint cmd/pgslice internal/app/pgslice

format:
	cd cmd/pgslice && go fmt
	cd internal/app/pgslice && go fmt

release:
	goreleaser --rm-dist

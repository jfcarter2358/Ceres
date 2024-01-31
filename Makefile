.PHONY: help build-docker build-local clean docs run-docker run-local test-regression test-stress test-unit

IMAGE_BUILT ?= false

# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help: ## Display this help message.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build-docker:  ## Build a CeresDB docker image
	docker build -t ceresdb .

build-local: clean  ## Build a local CeresDB binary
	mkdir dist
	cd ceresdb && env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o ceresdb
	mv ceresdb/ceresdb dist/ceresdb

build-release: build-local  ## Build release artifact tar
	cd dist && tar -czvf ../ceresdb-$$(cat ../ceresdb/VERSION).tar.gz .
	mv ceresdb-$$(cat ceresdb/VERSION).tar.gz ./dist

clean:  ## Remove build and test artifacts
	rm -rf dist || true
	docker-compose rm -f

docs:
	cd docs && make html

publish-docker: clean  ## Build and publish the CeresDB docker image
	docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v7 -t jfcarter2358/ceresdb:$$(cat ceresdb/VERSION) --push .

run-docker:  ## Run CeresDB in Docker
	docker run --rm -p 7437:7437 --name ceresdb --env "CERESDB_LOG_LEVEL=TRACE" ceresdb

run-local:  ## Run the local CeresDB binary
	cd dist; ./ceresdb

test-full: clean  ## Run full test suite against CeresDB
	make test-unit
	make test-regression IMAGE_BUILT=true
	make test-stress IMAGE_BUILT=true

test-regression:  ## Run regression tests against CeresDB
	@if [ "$(IMAGE_BUILT)" = "false" ] ; then \
        make build-docker ; \
    fi
	docker-compose rm -f
	docker-compose up &
	sleep 5
	cd test/regression && pytest
	docker-compose down
	
test-stress:  ## Run stress tests against CeresDB
	@if [ "$(IMAGE_BUILT)" = "false" ] ; then \
        make build-docker ; \
    fi
	docker-compose rm -f
	docker-compose up &
	sleep 5
	cd test/stress && pytest --durations=0
	docker-compose down

test-unit: clean  ## Run unit tests against CeresDB
	rm -rf /tmp/ceresdb || true
	mkdir -p /tmp/ceresdb/fixtures
	cd ceresdb && go test -p 1 -cover -coverprofile=../coverage.out ./...
	cd ceresdb && go tool cover -html=../coverage.out -o ../coverage.html 


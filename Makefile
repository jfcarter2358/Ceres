.PHONY: help build-docker build-local clean docs run-docker run-local test-regression test-stress test-unit

IMAGE_BUILT ?= false

# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help: ## Display this help message.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build-docker:  ## Build a CeresDB docker image
	docker build -t ceresdb .

build-local: clean  ## Build a local CeresDB binary
	rm -rf dist || true
	mkdir dist
	cd ceresdb && env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o ceresdb
	mv ceresdb/ceresdb dist/ceresdb
	cp -r template/.ceresdb dist/

build-release: build-local  ## Build release artifact tar
	cd dist && tar -czvf ceresdb-$$(cat ../ceresdb/VERSION).tar.gz .

clean:  ## Remove build and test artifacts
	ls test/.ceresdb/data/db1/foo | grep '-' | xargs -I % rm test/.ceresdb/data/db1/foo/% || true
	for dirname in bar bad baz ; do \
		rm test/.ceresdb/data/db1/foo/$$dirname || true ; \
	done
	for dirname in bar baz ; do \
		rm test/.ceresdb/data/db1/foo1/$$dirname || true ; \
	done
	rm -r test/.ceresdb/data/filter || true
	rm -r test/.ceresdb/data/action || true
	rm -r test/free_space_no_file || true
	rm -r test/.ceresdb/indices || true
	chmod -R +w test/.ceresdb-not-writable || true
	rm -r test/.ceresdb-not-writable || true
	rm -r dist || true
	rm -r test/empty || true
	rm test/.ceresdb*/free_space.json || true
	rm test/.ceresdb*/schema.json || true
	rm test/stress/timing_1000_*.json || true
	rm test/stress/*.png || true
	rm -r test/*/.*cache* || true
	rm -r test/*/*cache* || true
	docker kill ceresdb || true

docs:
	cd docs && make html

publish-docker: clean build-docker  ## Build and publish the CeresDB docker image
	docker tag ceresdb jfcarter2358/ceresdb:$$(cat ceresdb/VERSION)
	docker push jfcarter2358/ceresdb:$$(cat ceresdb/VERSION)

run-docker:  ## Run CeresDB in Docker
	docker run -p 7437:7437 ceresdb

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
	docker kill ceresdb || true
	docker run --rm -p 7437:7437 --name ceresdb -d ceresdb
	cd test/regression && pytest
	docker kill ceresdb || true

test-stress:  ## Run stress tests against CeresDB
	@if [ "$(IMAGE_BUILT)" = "false" ] ; then \
        make build-docker ; \
    fi
	docker kill ceresdb || true
	docker run --rm -p 7437:7437 --name ceresdb -d ceresdb
	cd test/stress && pytest --durations=0
	docker kill ceresdb || true

test-unit: clean  ## Run unit tests against CeresDB
	mkdir -p test/free_space_no_file
	mkdir -p test/.ceresdb/indices/db1/foo
	mkdir -p test/.ceresdb-not-writable/data
	mkdir -p test/.ceresdb-not-writable/indices/db1/foo
	mkdir -p test/empty/.ceresdb/config
	cp test/.ceresdb/config/aql.json test/empty/.ceresdb/config/aql.json
	chmod -R -w test/.ceresdb-not-writable
	for dirname in .ceresdb .ceresdb-schema .ceresdb-free_space .ceresdb-permit .ceresdb-user ; do \
		cp test/$$dirname/free_space.json.bak test/$$dirname/free_space.json ; \
		cp test/$$dirname/schema.json.bak test/$$dirname/schema.json ; \
	done
	for dirname in bar bad baz ; do \
		cp test/.ceresdb/data/db1/foo/$$dirname.bak test/.ceresdb/data/db1/foo/$$dirname ; \
	done
	for dirname in bar baz ; do \
		cp test/.ceresdb/data/db1/foo1/$$dirname.bak test/.ceresdb/data/db1/foo1/$$dirname ; \
	done
	cd ceresdb && go test -cover -coverprofile=../coverage.out ./...
	cd ceresdb && go tool cover -html=../coverage.out -o ../coverage.html 


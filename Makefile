.PHONY: build-docker build-local clean help run-docker run-local test-regression test-stress test-unit

build-docker:  ## Build a Ceres docker image
	docker build -t ceres .

build-local:  ## Build a local Ceres binary
	rm -rf dist || true
	mkdir dist
	cd ceres && env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o ceres
	mv ceres/ceres dist/ceres
	cp -r template/.ceres dist/

clean:  ## Remove build and test artifacts
	ls test/.ceres/data/db1/foo | grep '-' | xargs -I % rm test/.ceres/data/db1/foo/% || true
	rm test/.ceres/data/db1/foo/bar || true
	rm test/.ceres/data/db1/foo/bad || true
	rm test/.ceres/data/db1/foo/baz || true
	rm test/.ceres/data/db1/foo1/bar || true
	rm test/.ceres/data/db1/foo1/baz || true
	rm -r test/.ceres/data/filter || true
	rm -r test/.ceres/data/action || true
	rm -r test/free_space_no_file || true
	rm -r test/.ceres/indices || true
	chmod -R +w test/.ceres-not-writable || true
	rm -r test/.ceres-not-writable || true
	rm -r dist || true
	rm -r test/empty || true
	rm test/.ceres/free_space.json || true
	rm test/.ceres/schema.json || true
	rm test/stress/timing_1000_get.json || true
	rm test/stress/timing_1000_port.json || true
	rm test/stress/container.log || true
	rm -r test/stress/.pytest_cache || true
	rm -r test/stress/__pycache__ || true
	rm -r test/regression/.pytest_cache || true
	rm -r test/regression/__pycache__ || true

# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help: ## Display this help message.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

publish-docker: clean build-docker  ## Build and publish the Ceres docker image
	docker tag ceres jfcarter2358/ceres:$$(cat ceres/VERSION)
	docker push jfcarter2358/ceres:$$(cat ceres/VERSION)

run-docker:  ## Run Ceres in Docker
	docker run -p 7437:7437 ceres

run-local:  ## Run the local Ceres binary
	cd dist; ./ceres

test-regression: build-docker  ## Run regression tests against Ceres
	docker kill ceres || true
	docker rm ceres || true
	docker run -p 7437:7437 --name ceres -d ceres
	cd test/regression && pytest
	docker kill ceres || true
	docker rm ceres || true

test-stress: build-docker  ## Run stress tests against Ceres
	docker kill ceres || true
	docker rm ceres || true
	docker run -p 7437:7437 --name ceres -d ceres
	cd test/stress && pytest --durations=0
	docker kill ceres || true
	docker rm ceres || true

test-unit: clean  ## Run unit tests against Ceres
	mkdir -p test/free_space_no_file
	mkdir -p test/.ceres/indices/db1/foo
	mkdir -p test/.ceres-not-writable/data
	mkdir -p test/.ceres-not-writable/indices/db1/foo
	mkdir -p test/empty/.ceres/config
	cp test/.ceres/config/aql.json test/empty/.ceres/config/aql.json
	chmod -R -w test/.ceres-not-writable
	cp test/.ceres-schema/free_space.json.bak test/.ceres-schema/free_space.json
	cp test/.ceres-schema/schema.json.bak test/.ceres-schema/schema.json
	cp test/.ceres/free_space.json.bak test/.ceres/free_space.json
	cp test/.ceres/schema.json.bak test/.ceres/schema.json
	cp test/.ceres/data/db1/foo/bar.bak test/.ceres/data/db1/foo/bar
	cp test/.ceres/data/db1/foo/bad.bak test/.ceres/data/db1/foo/bad
	cp test/.ceres/data/db1/foo/baz.bak test/.ceres/data/db1/foo/baz
	cp test/.ceres/data/db1/foo1/bar.bak test/.ceres/data/db1/foo1/bar
	cp test/.ceres/data/db1/foo1/baz.bak test/.ceres/data/db1/foo1/baz
	cd ceres && go test -cover -coverprofile=../coverage.out ./...
	cd ceres && go tool cover -html=../coverage.out -o ../coverage.html 


.PHONY: dependencies run build-linux build-docker test

clean:
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

build-linux:
	# if building from a Mac you must install this first:
	# brew install FiloSottile/musl-cross/musl-cross
	rm -rf dist || true
	mkdir dist
	cd ceres&&  env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o ceres
	mv ceres/ceres dist/ceres
	cp -r template/.ceres dist/

run:
	cd dist; ./ceres

test: clean
	mkdir -p test/free_space_no_file
	mkdir -p test/.ceres/indices/db1/foo
	mkdir -p test/.ceres-not-writable/data
	mkdir -p test/.ceres-not-writable/indices/db1/foo
	mkdir -p test/empty/.ceres/config
	cp test/.ceres/config/aql.json test/empty/.ceres/config/aql.json
	chmod -R -w test/.ceres-not-writable
	cp test/.ceres/free_space.json.bak test/.ceres/free_space.json
	cp test/.ceres/schema.json.bak test/.ceres/schema.json
	cp test/.ceres/data/db1/foo/bar.bak test/.ceres/data/db1/foo/bar
	cp test/.ceres/data/db1/foo/bad.bak test/.ceres/data/db1/foo/bad
	cp test/.ceres/data/db1/foo/baz.bak test/.ceres/data/db1/foo/baz
	cp test/.ceres/data/db1/foo1/bar.bak test/.ceres/data/db1/foo1/bar
	cp test/.ceres/data/db1/foo1/baz.bak test/.ceres/data/db1/foo1/baz
	cd ceres && go test -cover -coverprofile=../coverage.out ./...
	cd ceres && go tool cover -html=../coverage.out -o ../coverage.html 

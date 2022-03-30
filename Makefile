.PHONY: dependencies run build-linux build-docker test

clean:
	ls test/.ceres/data/db1/foo | grep '-' | xargs -I % rm test/.ceres/data/db1/foo/% || true
	rm test/.ceres/data/db1/foo/bar || true
	rm test/.ceres/data/db1/foo/bad || true
	rm test/.ceres/data/db1/foo/baz || true
	rm test/.ceres/data/db1/foo1/bar || true
	rm test/.ceres/data/db1/foo1/baz || true
	rm -r test/free_space_no_file || true

build-linux:
	# if building from a Mac you must install this first:
	# brew install FiloSottile/musl-cross/musl-cross
	rm -rf dist || true
	mkdir dist
	cd ceres&&  env GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -ldflags "-linkmode external -extldflags -static" -v -o ceres
	mv ceres/ceres dist/ceres

run:
	cd dist; ./ceres

test: clean
	mkdir -p test/free_space_no_file
	cp test/.ceres/data/db1/foo/bar.bak test/.ceres/data/db1/foo/bar
	cp test/.ceres/data/db1/foo/bad.bak test/.ceres/data/db1/foo/bad
	cp test/.ceres/data/db1/foo/baz.bak test/.ceres/data/db1/foo/baz
	cp test/.ceres/data/db1/foo1/bar.bak test/.ceres/data/db1/foo1/bar
	cp test/.ceres/data/db1/foo1/baz.bak test/.ceres/data/db1/foo1/baz
	cd ceres && go test -cover -coverprofile=../coverage.out ./...
	cd ceres && go tool cover -html=../coverage.out -o ../coverage.html 

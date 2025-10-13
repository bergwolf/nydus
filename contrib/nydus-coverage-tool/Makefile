PACKAGES ?= $(shell go list ./... | grep -v /vendor/)
GOARCH ?= $(shell go env GOARCH)
GOPROXY ?=

ifdef GOPROXY
PROXY := GOPROXY=${GOPROXY}
endif

.PHONY: all build release test clean

all: build

build:
	@CGO_ENABLED=0 ${PROXY} GOOS=linux GOARCH=${GOARCH} go build -o ./coverage-tool ./cmd/coverage-tool/main.go

release:
	@CGO_ENABLED=0 ${PROXY} GOOS=linux GOARCH=${GOARCH} go build -ldflags '-s -w -extldflags "-static"' -o ./coverage-tool ./cmd/coverage-tool/main.go

test:
	@go vet $(PACKAGES)
	@go test -count=1 -v -timeout 10m $(PACKAGES)

clean:
	rm -f coverage-tool
	rm -f cmd/coverage-tool/main

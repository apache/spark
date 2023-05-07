FIRST_GOPATH              := $(firstword $(subst :, ,$(GOPATH)))
PKGS                      := $(shell go list ./... | grep -v /tests | grep -v /xcpb | grep -v /gpb)
GOFILES_NOVENDOR          := $(shell find . -name vendor -prune -o -type f -name '*.go' -not -name '*.pb.go' -print)
GOFILES_BUILD             := $(shell find . -type f -name '*.go' -not -name '*_test.go')
PROTOFILES                := $(shell find . -name vendor -prune -o -type f -name '*.proto' -print)

ALLGOFILES				  := $(shell find . -type f -name '*.go')
DATE                      := $(shell date -u -d "@$(SOURCE_DATE_EPOCH)" '+%FT%T%z' 2>/dev/null || date -u '+%FT%T%z')

BUILDFLAGS_NOPIE		  :=
#BUILDFLAGS_NOPIE          := -trimpath -ldflags="-s -w -X main.version=$(GOPASS_VERSION) -X main.commit=$(GOPASS_REVISION) -X main.date=$(DATE)" -gcflags="-trimpath=$(GOPATH)" -asmflags="-trimpath=$(GOPATH)"
BUILDFLAGS                ?= $(BUILDFLAGS_NOPIE) -buildmode=pie
TESTFLAGS                 ?=
PWD                       := $(shell pwd)
PREFIX                    ?= $(GOPATH)
BINDIR                    ?= $(PREFIX)/bin
GO                        := GO111MODULE=on go
GOOS                      ?= $(shell go version | cut -d' ' -f4 | cut -d'/' -f1)
GOARCH                    ?= $(shell go version | cut -d' ' -f4 | cut -d'/' -f2)
TAGS                      ?= netgo
SHELL = bash

BINARIES				  := cmd/spark-connect-example-raw-grpc-client/spark-connect-example-raw-grpc-client cmd/spark-connect-example-spark-session/spark-connect-example-spark-session

# Define the location of SPARK_HOME because we need that to depend on the build paths
MAKEFILE_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

PROTO_SRC = $(shell find internal/generated -type f -name *.proto )
REGEN_SCRIPT = $(MAKEFILE_DIR)/../../../../dev/connect-gen-protos.sh


OK := $(shell tput setaf 6; echo ' [OK]'; tput sgr0;)

all: build

build: $(BUILD_OUTPUT) $(BINARIES)

internal/generated.out:
	@echo -n ">> BUILD, output = $@"
	@$(REGEN_SCRIPT)
	@touch internal/generated.out
	@printf '%s\n' '$(OK)'

$(GOFILES_BUILD): internal/generated.out

$(BUILD_OUTPUT): $(GOFILES_BUILD)
	@echo -n ">> BUILD, output = $@"
	@$(GO) build -o $@ $(BUILDFLAGS)
	@printf '%s\n' '$(OK)'


lint: $(BUILD_OUTPUT)
	@golangci-lint run

fmt:
	@gofumpt -extra -w $(ALLGOFILES)

test: $(BUILD_OUTPUT)
	@echo ">> TEST, \"verbose\""
	@$(foreach pkg, $(PKGS),\
	    @echo -n "     ";\
		$(GO) test -v -run '(Test|Example)' $(BUILDFLAGS) $(TESTFLAGS) $(pkg) || exit 1)

fulltest: $(BUILD_OUTPUT)
	@echo ">> TEST, \"coverage\""
	@echo "mode: atomic" > coverage-all.out
	@$(foreach pkg, $(PKGS),\
	    echo -n "     ";\
		go test -run '(Test|Example)' $(BUILDFLAGS) $(TESTFLAGS) -coverprofile=coverage.out -covermode=atomic $(pkg) || exit 1;\
		tail -n +2 coverage.out >> coverage-all.out;)
	@$(GO) tool cover -html=coverage-all.out -o coverage-all.html


cmd/spark-connect-example-raw-grpc-client/spark-connect-example-raw-grpc-client: $(GOFILES_BUILD)
	@echo ">> BUILD, output = $@"
	@cd $(dir $@) && $(GO) build -o $(notdir $@) $(BUILDFLAGS)
	@printf '%s\n' '$(OK)'

cmd/spark-connect-example-spark-session/spark-connect-example-spark-session: $(GOFILES_BUILD)
	@echo ">> BUILD, output = $@"
	@cd $(dir $@) && $(GO) build -o $(notdir $@) $(BUILDFLAGS)
	@printf '%s\n' '$(OK)'


clean:
	@echo -n ">> CLEAN"
	@$(GO) clean -i ./...
	@rm -rf ./internal/generated
	@rm  -f ./internal/generated.out
	@rm -f ./coverage-all.html
	@rm -f ./coverage-all.out
	@rm -f ./coverage.out
	@find . -type f -name "coverage.out" -delete
	@printf '%s\n' '$(OK)'
.PHONY: build test fmt
GO      ?= go
PKG := ./pkg/...

BUILDTARGET := bin/br
SUFFIX := $(GOEXE)

GITSHA := $(shell git describe --no-match --always --dirty)
GITREF := $(shell git rev-parse --abbrev-ref HEAD)

REPO := github.com/vesoft-inc/nebula-br

LDFLAGS += -X $(REPO)/pkg/version.GitSha=$(GITSHA)
LDFLAGS += -X $(REPO)/pkg/version.GitRef=$(GITREF)

build:
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BUILDTARGET) main.go
	chmod +x $(BUILDTARGET)

test:
	$(GO) test -v $(PKG) -short

fmt:
	$(GO) mod tidy && find . -path vendor -prune -o -type f -iname '*.go' -exec goimports -w {} \;


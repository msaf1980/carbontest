NAME:=carbontest

VERSION := $(shell git describe --always --tags)

GO ?= go

all: $(NAME)
static: $(NAME)-static

FORCE:

$(NAME): FORCE
	$(GO) build -ldflags "-X main.version=${VERSION}" ./cmd/${NAME}

$(NAME)-static: FORCE
	CGO_ENABLED=0 $(GO) build -ldflags "-X main.version=${VERSION}" ./cmd/${NAME}

debug: FORCE
	$(GO) build -gcflags=all='-N -l' -ldflags "-X main.version=${VERSION}" ./cmd/${NAME}

test: FORCE
	$(GO) test -coverprofile coverage.txt  ./...

clean:
	@rm -f ./${NAME}

lint:
	golangci-lint run

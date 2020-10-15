NAME:=carbontest

VERSION := $(shell git describe --always --tags)

GO ?= go

all: $(NAME)

FORCE:

$(NAME): FORCE
	$(GO) build -ldflags "-X main.version=${VERSION}"

debug: FORCE
	$(GO) build -gcflags=all='-N -l' -ldflags "-X main.version=${VERSION}"

test: FORCE
	$(GO) test -coverprofile coverage.txt  ./...

clean:
	@rm -f ./${NAME}

lint:
	golangci-lint run

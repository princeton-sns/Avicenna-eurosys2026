CURR_DIR = $(shell pwd)
BIN_DIR = bin
export GOPATH=$(CURR_DIR)
export GO111MODULE=off
#GO_BUILD = env GOOS=linux GOARCH=amd64 GOBIN=$(CURR_DIR)/$(BIN_DIR) go install $@
GO_BUILD = env GOOS=linux GOARCH=amd64 go install $@

all: server master clientmain clientol

server:
	$(GO_BUILD)

client:
	$(GO_BUILD)

master:
	$(GO_BUILD)

clientmain:
	$(GO_BUILD)

clientol:
	$(GO_BUILD)

.PHONY: clean

clean:
	rm -rf bin pkg

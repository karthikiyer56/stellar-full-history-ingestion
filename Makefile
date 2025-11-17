##############
PROTO_DIR := protos
GEN_SUFFIX := .pb.go
PROTO_FILES := $(shell find $(PROTO_DIR) -name "*.proto")
PROTO_CHECKSUM := .proto_checksums

generate-proto:
	@echo "Regenerating proto files..."
	@touch $(PROTO_CHECKSUM)
	@current_checksum=$$(sha256sum $(PROTO_FILES) | sha256sum | awk '{print $$1}'); \
	stored_checksum=$$(cat $(PROTO_CHECKSUM)); \
	if [ "$${current_checksum}" != "$${stored_checksum}" ]; then \
    	echo "Changes detected. Regenerating all proto files..."; \
    	MAP_OPTS=$$(for file in $(PROTO_FILES); do \
    		rel_path=$$(echo $$file | sed 's|$(PROTO_DIR)/||'); \
    		pkg_path=$$(dirname $$rel_path); \
    		go_pkg="github.com/stellar/go/$$pkg_path"; \
    		printf "M%s=%s," "$$rel_path" "$$go_pkg"; \
    	done); \
    	MAP_OPTS=$${MAP_OPTS%,}; \
    	echo "Running protoc with options:"; \
    	echo "  --go_out=."; \
    	echo "  --go_opt=paths=source_relative"; \
    	echo "  --go_opt=$$MAP_OPTS"; \
    	echo "Proto Files:  $(PROTO_FILES)"; \
    	protoc -I=$(PROTO_DIR) \
    	       --go_out=. --go_opt=paths=source_relative \
    	       --go_opt=$$MAP_OPTS \
    	       $(PROTO_FILES); \
    	echo "$${current_checksum}" > $(PROTO_CHECKSUM); \
    else \
    	echo "No changes detected in proto files."; \
    fi


regenerate-proto: $(PROTO_CHECKSUM)
	rm -f $(PROTO_CHECKSUM)
	$(MAKE) generate-proto

$(PROTO_CHECKSUM):
	@touch $(PROTO_CHECKSUM)

.PHONY: generate-proto regenerate-proto


##############
# MDBX Build
##############
MDBX_HOME := $(HOME)/local/libmdbx
MDBX_BINARY := $(HOME)/bin/full_mdbx_ingestion

# CGO settings for MDBX
CGO_ENABLED := 1
CGO_CFLAGS := -I$(MDBX_HOME)/include
CGO_LDFLAGS := -L$(MDBX_HOME)/lib -lmdbx -Wl,-rpath,$(MDBX_HOME)/lib

# Mac specific
GOARCH := arm64
GOOS := darwin

.PHONY: build-mdbx clean-mdbx test-mdbx run-mdbx check-mdbx-env

build-mdbx:
	@echo "🔨 Building MDBX ingestion binary..."
	CGO_ENABLED=$(CGO_ENABLED) \
	GOARCH=$(GOARCH) \
	GOOS=$(GOOS) \
	CGO_CFLAGS="$(CGO_CFLAGS)" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go build -o $(MDBX_BINARY) mdbx/ingestion/full_mdbx_ingestion.go
	@echo "✅ Binary created at: $(MDBX_BINARY)"

clean-mdbx:
	@echo "🧹 Cleaning MDBX binaries..."
	rm -f $(MDBX_BINARY)
	@echo "✅ Clean complete"

test-mdbx:
	@echo "Testing MDBX installation..."
	@echo "MDBX_HOME: $(MDBX_HOME)"
	@echo ""
	@echo "Checking files..."
	@test -f $(MDBX_HOME)/lib/libmdbx.dylib && echo "  ✅ libmdbx.dylib found" || echo "  ❌ libmdbx.dylib NOT found"
	@test -f $(MDBX_HOME)/lib/libmdbx.a && echo "  ✅ libmdbx.a found" || echo "  ❌ libmdbx.a NOT found"
	@test -f $(MDBX_HOME)/include/mdbx.h && echo "  ✅ mdbx.h found" || echo "  ❌ mdbx.h NOT found"
	@echo ""
	@echo "Testing Go compilation..."
	@echo 'package main; import ("fmt"; "github.com/erigontech/mdbx-go/mdbx"); func main() { env, err := mdbx.NewEnv(); if err != nil { panic(err) }; defer env.Close(); fmt.Println("✅ MDBX working!") }' > /tmp/test_mdbx.go
	@CGO_ENABLED=1 \
	CGO_CFLAGS="$(CGO_CFLAGS)" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	DYLD_LIBRARY_PATH=$(MDBX_HOME)/lib \
	go run /tmp/test_mdbx.go
	@rm /tmp/test_mdbx.go

run-mdbx: build-mdbx
	@echo "Running MDBX ingestion..."
	DYLD_LIBRARY_PATH=$(MDBX_HOME)/lib ./$(MDBX_BINARY)

check-mdbx-env:
	@echo "Environment Check:"
	@echo "  MDBX_HOME: $(MDBX_HOME)"
	@echo "  CGO_ENABLED: $(CGO_ENABLED)"
	@echo "  CGO_CFLAGS: $(CGO_CFLAGS)"
	@echo "  CGO_LDFLAGS: $(CGO_LDFLAGS)"
	@echo "  GOARCH: $(GOARCH)"
	@echo "  GOOS: $(GOOS)"
	@echo ""
	@echo "Files Check:"
	@test -f $(MDBX_HOME)/lib/libmdbx.dylib && echo "  ✅ libmdbx.dylib" || echo "  ❌ libmdbx.dylib NOT FOUND"
	@test -f $(MDBX_HOME)/lib/libmdbx.a && echo "  ✅ libmdbx.a" || echo "  ❌ libmdbx.a NOT FOUND"
	@test -f $(MDBX_HOME)/include/mdbx.h && echo "  ✅ mdbx.h" || echo "  ❌ mdbx.h NOT FOUND"
	@echo ""
	@echo "Source Check:"
	@test -f mdbx/ingestion/full_mdbx_ingestion.go && echo "  ✅ full_mdbx_ingestion.go" || echo "  ❌ full_mdbx_ingestion.go NOT FOUND"
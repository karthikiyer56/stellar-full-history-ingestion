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
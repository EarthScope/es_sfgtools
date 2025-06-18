# Detect system OS and ARCH dynamically
OS := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)

TEST_PREFIX := $(shell echo $$CONDA_PREFIX)

# Variables
BUILD_DIR=build
COMMANDS=$(wildcard cmd/*)
CONDA_BASE := $(shell conda info --base)
CONDA_PREFIX := $(shell echo $$CONDA_PREFIX)

# Set DYLD_LIBRARY_PATH on macOS or LD_LIBRARY_PATH on Linux
ifeq ($(OS),darwin)
    DYLD_LIBRARY_PATH := $(CONDA_PREFIX)/lib:$(DYLD_LIBRARY_PATH)
else
    LD_LIBRARY_PATH := $(CONDA_PREFIX)/lib:$(LD_LIBRARY_PATH)
endif

# CGO flags for TileDB
CGO_CFLAGS = -I$(CONDA_PREFIX)/include -Wno-deprecated-declarations
CGO_LDFLAGS = -L$(CONDA_PREFIX)/lib -ltiledb

LIBRARY_PATH = $(CONDA_PREFIX)/lib
CPATH = $(CONDA_PREFIX)/include
CLANG_PATH = $(CONDA_PREFIX)/bin/clang
PSEUDO_PATH = $(CLANG_PATH)
CC = clang

# Build all commands
build: $(COMMANDS)
	@mkdir -p $(BUILD_DIR)
	@for cmd in $(COMMANDS); do \
		CMD_NAME=$$(basename "$$cmd"); \
		echo "Command Name: $$CMD_NAME"; \
		BINARY_NAME=$${CMD_NAME}_$(OS)_$(ARCH); \
		echo "Building $$BINARY_NAME for $(OS)/$(ARCH)..."; \
		env CGO_ENABLED=1 GOOS=$(OS) GOARCH=$(ARCH) CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" DYLD_LIBRARY_PATH=$(DYLD_LIBRARY_PATH) LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) go build -o $(BUILD_DIR)/$$BINARY_NAME ./cmd/$$CMD_NAME; \
	done

debug:
	@echo "SHELL = $(SHELL)"
	@echo "OS: $(OS)"
	@echo "ARCH: $(ARCH)"
	@echo "TEST_PREFIX: $(TEST_PREFIX)"
	@echo "LIBRARY_PATH: $(LIBRARY_PATH)"
	@echo "Conda Prefix: $(CONDA_PREFIX)"
	@echo "Conda Base: $(CONDA_BASE)"
	@echo "TileDB Library Path: $(CONDA_PREFIX)/lib"
	@echo "DYLD_LIBRARY_PATH: $(DYLD_LIBRARY_PATH)"
	@echo "CPATH: $(CPATH)"
	@for cmd in $(COMMANDS); do \
		echo "Processing: $$cmd"; \
		CMD_NAME=$$(basename "$$cmd"); \
		echo "Command Name: "$$CMD_NAME"";\
		BINARY_NAME=$${CMD_NAME}_$(OS)_$(ARCH); \
		echo "Binary Name: $$BINARY_NAME"; \
	done
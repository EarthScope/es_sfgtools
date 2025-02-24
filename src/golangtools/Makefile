# Detect system OS and ARCH dynamically
OS := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)

# Variables
BUILD_DIR=build
COMMANDS=$(wildcard cmd/*)
CONDA_BASE := $(shell conda info --base)
#CONDA_PREFIX := $(shell conda info --envs | awk '/\*/ {print $3}' | tr -d ' ')
#CONDA_PREFIX=/Users/franklyndunbar/miniconda3/envs/seafloor_geodesy_mac
CONDA_PREFIX := $(shell conda info --envs | awk '/\*/ {print $$NF}')

DYLD_LIBRARY_PATH := $(shell echo $$DYLD_LIBRARY_PATH)
DYLD_LIBRARY_PATH_CONDA=$(CONDA_PREFIX)/include:$(DYLD_LIBRARY_PATH)
CGO_CFLAGS=-I$(CONDA_BASE)/tiledb/include
CGO_LDFLAGS=-L$(CONDA_PREFIX)/lib -ltiledb

LIBRARY_PATH=$(CONDA_PREFIX)/lib
CPATH=$(CONDA_PREFIX)/include
CLANG_PATH=$(CONDA_PREFIX)/bin/clang
PSEUDO_PATH=$(CLANG_PATH)
CC=clang
# Build all commands
#LIBRARY_PATH=$(LIBRARY_PATH) CPATH=$(CPATH) CGO_ENABLED=1 CGO_CFLAGS=$(CGO_CFLAGS)  DYLD_LIBRARY_PATH=$(DYLD_LIBRARY_PATH_CONDA)

build: $(COMMANDS)
	@mkdir -p $(BUILD_DIR)
	@for cmd in $(COMMANDS); do \
		CMD_NAME=$$(basename "$$cmd"); \
		echo "Command Name: $$CMD_NAME"; \
		BINARY_NAME=$${CMD_NAME}_$(OS)_$(ARCH); \
		echo "Building $$BINARY_NAME for $(OS)/$(ARCH)..."; \
		env GOOS=$(OS) GOARCH=$(ARCH) go build -o $(BUILD_DIR)/$$BINARY_NAME ./cmd/$$CMD_NAME; \
	done


debug:
	@echo "OS: $(OS)"
	@echo "ARCH: $(ARCH)"
	@echo "LIBRARY_PATH: $(LIBRARY_PATH)"
	@echo "Conda Prefix: $(CONDA_PREFIX)"
	@echo "TileDB Library Path: $(CONDA_PREFIX)/lib"
	@echo "DYLD_LIBRARY_PATH: $(DYLD_LIBRARY_PATH_CONDA)"
	@echo "CPATH: $(CPATH)"
	@for cmd in $(COMMANDS); do \
		echo "Processing: $$cmd"; \
		CMD_NAME=$$(basename "$$cmd"); \
		echo "Command Name: "$$CMD_NAME"";\
		BINARY_NAME=$${CMD_NAME}_$(OS)_$(ARCH); \
		echo "Binary Name: $$BINARY_NAME"; \
	done
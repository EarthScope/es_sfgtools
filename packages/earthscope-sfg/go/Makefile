# Detect system OS and ARCH dynamically
OS := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)

TEST_PREFIX := $(shell echo $$CONDA_PREFIX)

# Variables
BUILD_DIR=build
COMMANDS=$(wildcard cmd/*)
# Use existing CONDA_PREFIX (from conda or micromamba); avoid calling `conda` directly
CONDA_PREFIX := $(shell echo $$CONDA_PREFIX)

# Set DYLD_LIBRARY_PATH on macOS or LD_LIBRARY_PATH on Linux
ifeq ($(OS),darwin)
    DYLD_LIBRARY_PATH := $(CONDA_PREFIX)/lib:$(DYLD_LIBRARY_PATH)
    # Embed @executable_path/libs so bundled dylibs are found without conda
    RPATH_FLAG = -Wl,-rpath,@executable_path/libs
else
    LD_LIBRARY_PATH := $(CONDA_PREFIX)/lib:$(LD_LIBRARY_PATH)
    # On Linux embed $ORIGIN/libs so bundled .so files are found without conda
    RPATH_FLAG = -Wl,-rpath,$$ORIGIN/libs
endif

# CGO flags for TileDB
CGO_CFLAGS = -I$(CONDA_PREFIX)/include -Wno-deprecated-declarations
CGO_LDFLAGS = -L$(CONDA_PREFIX)/lib -ltiledb $(RPATH_FLAG)

LIBRARY_PATH = $(CONDA_PREFIX)/lib
CPATH = $(CONDA_PREFIX)/include
CLANG_PATH = $(CONDA_PREFIX)/bin/clang
PSEUDO_PATH = $(CLANG_PATH)
CC = clang

# Build all commands
# -buildvcs=false was added 8-21-25 to fix an issue building on AMD64 architecture
build: $(COMMANDS)
	@mkdir -p $(BUILD_DIR)
	@for cmd in $(COMMANDS); do \
		CMD_NAME=$$(basename "$$cmd"); \
		echo "Command Name: $$CMD_NAME"; \
		BINARY_NAME=$${CMD_NAME}_$(OS)_$(ARCH); \
		echo "Building $$BINARY_NAME for $(OS)/$(ARCH)..."; \
		env CGO_ENABLED=1 GOOS=$(OS) GOARCH=$(ARCH) CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" DYLD_LIBRARY_PATH=$(DYLD_LIBRARY_PATH) LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) go build -o $(BUILD_DIR)/$$BINARY_NAME -buildvcs=false ./cmd/$$CMD_NAME; \
	done

# Bundle all binaries with their dylib dependencies so they run without conda.
# On macOS: copies libtiledb.dylib and its transitive deps into build/libs/,
#           rewrites all @rpath references to be self-contained, and strips the
#           absolute conda RPATH from each binary.
# On Linux: copies .so files into build/libs/ and uses patchelf to set RPATH.
#
# Requires: Python 3 (macOS); patchelf (Linux, install via conda: conda install patchelf)
bundle: build
ifeq ($(OS),darwin)
	@echo "Bundling dylibs for macOS..."
	@python3 $(CURDIR)/bundle_dylibs.py \
		--build-dir $(CURDIR)/$(BUILD_DIR) \
		--conda-lib $(CONDA_PREFIX)/lib \
		--os $(OS) \
		--arch $(ARCH)
else
	@echo "Bundling .so files for Linux..."
	@python3 $(CURDIR)/bundle_dylibs.py \
		--build-dir $(CURDIR)/$(BUILD_DIR) \
		--conda-lib $(CONDA_PREFIX)/lib \
		--os $(OS) \
		--arch $(ARCH)
endif
	@echo "Bundle complete. Contents:"
	@du -sh $(BUILD_DIR)/libs/
	@echo "Binaries are self-contained — no conda required at runtime."

debug:
	@echo "SHELL = $(SHELL)"
	@echo "OS: $(OS)"
	@echo "ARCH: $(ARCH)"
	@echo "TEST_PREFIX: $(TEST_PREFIX)"
	@echo "LIBRARY_PATH: $(LIBRARY_PATH)"
	@echo "Conda Prefix: $(CONDA_PREFIX)"
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
# earthscope-sfg

EarthScope seafloor geodesy parsing — NovaTel, Sonardyne, and shared data models.

## Installation

### Python (core parsing)

```bash
pip install earthscope-sfg
```

### Python with TileDB support

```bash
pip install earthscope-sfg[tiledb]
```

### Development (monorepo)

```bash
pixi install          # installs all packages in editable mode
pixi run setup        # builds Go binaries + compiles GARPOS + installs PRIDE
```

## Go CLI Binaries

The package includes Go programs for high-throughput NovaTel conversion and
RINEX quality-checking. These are **optional** — the pure-Python parsers
(`novatel_tools`, `sonardyne_tools`) work without them.

| Binary | Description |
|--------|-------------|
| `nova2rnx` | NovaTel ASCII → RINEX |
| `novb2rnx` | NovaTel binary (770) → RINEX |
| `nov0002rnx` | NovaTel binary (000) → RINEX |
| `nova2tile` | NovaTel ASCII → TileDB |
| `novab2tile` | NovaTel binary (770) → TileDB |
| `nov0002tile` | NovaTel binary (000) → TileDB |
| `tdb2rnx` | TileDB → RINEX (roundtrip) |
| `rnxqc` | RINEX quality check (multipath, ionospheric jumps) |

### Build prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| **Go** | ≥ 1.24 | [go.dev/dl](https://go.dev/dl/) |
| **TileDB C library** | ≥ 2.26 | Headers and shared library. Provided by `conda install tiledb` or the [TileDB install guide](https://docs.tiledb.com/main/how-to/installation). |
| **C compiler** | clang or gcc | Required by CGO for the TileDB Go bindings. |
| **make** | any | Used by the Makefile build target. |

The Makefile expects the TileDB headers and library to be available via
`$CONDA_PREFIX`. If you installed TileDB elsewhere, override the paths:

```bash
export CONDA_PREFIX=/path/to/tiledb/prefix   # must contain include/ and lib/
```

### Building

From the repository root:

```bash
# Using pixi (recommended — sets up the full environment):
pixi run build-novatel-go

# Or directly with make:
cd packages/earthscope-sfg
make -C go build
```

This produces platform-specific binaries in `go/build/`:

```
go/build/
  nova2rnx_darwin_arm64
  nova2rnx_linux_amd64
  ...
```

The Python layer resolves the correct binary at runtime based on
`platform.system()` and `platform.machine()`.

### Build troubleshooting

**`fatal error: 'tiledb/tiledb.h' file not found`**
TileDB C headers are not on the include path. Verify `$CONDA_PREFIX/include/tiledb/tiledb.h` exists, or set `CGO_CFLAGS` manually:

```bash
export CGO_CFLAGS="-I/path/to/tiledb/include"
export CGO_LDFLAGS="-L/path/to/tiledb/lib -ltiledb"
```

**`ld: library 'tiledb' not found`**
The TileDB shared library is not on the linker path. Verify `$CONDA_PREFIX/lib/libtiledb.*` exists.

**`dyld: Library not loaded: libtiledb.dylib` (runtime)**
Set the dynamic library path before running a binary:

```bash
# macOS
export DYLD_LIBRARY_PATH=$CONDA_PREFIX/lib:$DYLD_LIBRARY_PATH

# Linux
export LD_LIBRARY_PATH=$CONDA_PREFIX/lib:$LD_LIBRARY_PATH
```

### Go module path

The Go module is declared as:

```
module github.com/EarthScope/es_sfgtools/src/golangtools
```

Internal packages live under `go/pkg/sfg_utils/`.

## Python API overview

| Module | Description |
|--------|-------------|
| `es_sfgtools.novatel_tools` | NovaTel GNSS log parsing (RangeA, ASCII, binary) and RINEX conversion |
| `es_sfgtools.sonardyne_tools` | Sonardyne SV3 acoustic data parsing and QC |
| `es_sfgtools.data_models` | Pandera schemas (`ShotDataFrame`, `AcousticDataFrame`), Pydantic models, constants |
| `es_sfgtools.tiledb_schemas` | TileDB array schemas and typed array classes *(requires `[tiledb]` extra)* |
| `es_sfgtools.seafloor_site_tools` | Sound velocity profile processing |
| `es_sfgtools.utils` | Binary path resolution, config merging, CLI log parsing |

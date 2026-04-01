# PRD: Mono-Repository Package Split

**Date:** 2026-03-31  
**Authors:** Franklyn Dunbar  
**Status:** Draft

---

## Problem Statement

The `es_sfgtools` repository currently ships as a single monolithic Python package. All concerns — CLI commands, workflow orchestration, data parsing, data management, modeling, configuration, and shared types — live under one `src/es_sfgtools/` namespace installed as a single distribution.

This creates several problems:

1. **Heavy install footprint.** A user who only needs to parse NovaTel RangeA data must install GARPOS, TileDB, boto3, earthscope-sdk, and dozens of other dependencies they will never use.
2. **Tight coupling.** The `WorkflowHandler` class acts as a "god object," importing nearly every subpackage. The `config` module imports backward into `prefiltering` and `modeling`, creating circular-dependency risk. Changes in one domain frequently cascade into unrelated domains.
3. **Testing friction.** Running the parser unit tests requires the entire dependency tree (TileDB, boto3, etc.) to be installed, even though the parsers themselves are lightweight.
4. **Contribution friction.** Contributors working on CLI features must understand the workflow internals, and vice versa. The blast radius of any change is the entire package.
5. **Deployment inflexibility.** The CLI, the library, and the modeling tools cannot be versioned or deployed independently. A cloud Lambda that only runs parsing still needs the full package.

## Solution

Convert the repository into a **mono-repo** containing three independently-installable packages under a shared `es_sfgtools` namespace. The axes of separation are:

- **`earthscope-sfg`** — all vendor-specific parsing, generic seafloor tools, shared data models, and utility code. This is a single package that houses:
  - **NovaTel** — GNSS receiver log parsing (Python `novatel_tools` + Go CLI binaries `nova2rnx`, `novb2rnx`, etc.)
  - **Sonardyne** — SV2/SV3 underwater acoustic positioning data parsing and QC
  - **PRIDE-PPPAR** — GNSS post-processing product retrieval and RINEX utilities
  - **Seafloor Site Tools** — generic sound velocity profile processing
  - **Data Models & Constants** — shared Pydantic schemas, observables, constants, and community standards
  - **Utilities** — cross-cutting helpers (CLI log parsing, custom exceptions, model update, RINEX histogram)
- **`es-sfgtools-workflows`** — stateful orchestration of pipelines, data management, and modeling
- **`es-sfgtools-cli`** — user-facing command-line application that drives workflows via manifests

Consolidating all parsing and data model code into a single `earthscope-sfg` package avoids code duplication while still cleanly separating parsing concerns from orchestration. Vendor modules remain in their own sub-namespaces (`novatel_tools`, `sonardyne_tools`, `pride_tools`) so import paths are stable and the code is well-organized, but they share utilities, constants, and data models without needing cross-package dependencies. This also eliminates the Sonardyne → NovaTel cross-package dependency — both simply coexist in the same package.

The packages will live in the same Git repository, share a single CI pipeline, and be managed by **pixi workspaces** (already in use) with per-package `pyproject.toml` files. The Go binaries are built per-platform by the NovaTel package's pixi tasks and bundled alongside the Python code.

---

## User Stories

1. As a **data engineer**, I want to install only the parsing package (`earthscope-sfg`) so that I can convert RangeA logs to RINEX and parse SV3 data in a lightweight container without pulling in TileDB, boto3, or GARPOS.
2. As a **data engineer**, I want `pip install earthscope-sfg` to also provide the pre-built Go CLI binaries (`nova2rnx`, `novb2rnx`, `nov0002rnx`) for my platform so that I have a single install step for all NovaTel parsing.
3. As a **data engineer**, I want the parsing package to have zero network dependencies so that I can run parsing in air-gapped environments.
4. As a **scientist**, I want to install the full workflow package (`es-sfgtools-workflows`) and get end-to-end processing (ingest → preprocess → model) with a single `pip install`.
5. As a **scientist**, I want import paths to remain stable (e.g. `from es_sfgtools.novatel_tools import deserialize_rangea`) so that my existing scripts keep working after the split.
6. As an **operator**, I want to install the CLI (`es-sfgtools-cli`) and run `sfgtools run manifest.yaml` without writing Python code.
7. As an **operator**, I want the CLI to declare its workflow dependency so that `pip install es-sfgtools-cli` transitively installs everything needed to execute manifests.
8. As a **developer**, I want each package to declare only its own direct dependencies in its `pyproject.toml` so that I can reason about what a package needs.
9. As a **developer**, I want `pixi install` at the repo root to install all packages in editable mode so that local development still feels like a single project.
10. As a **developer**, I want to run `pytest tests/` per package so that I can validate NovaTel parsing changes without a GARPOS compilation step.
11. As a **developer**, I want all vendor-specific parsing, data models, constants, and utilities in a single `earthscope-sfg` package so that there is no code duplication and no cross-package dependency gymnastics between parsers.
12. As a **CI system**, I want each package to have its own test matrix so that a parsing-only PR does not trigger GARPOS or workflow tests.
13. As a **release manager**, I want all packages to share a single version derived from Git tags (`setuptools_scm`) so that compatibility between installed packages is trivially verifiable.
14. As a **developer**, I want the dependency graph between packages to be a strict DAG so that circular imports are structurally impossible.
15. As a **developer**, I want the `config` and `logging` modules in the `workflows` package so that vendor parsers remain stateless and free of orchestration concerns.
16. As a **scientist**, I want a convenience meta-package (`es-sfgtools`) that installs everything so that the default experience is unchanged.
17. As a **developer**, I want GARPOS Fortran compilation to remain a pixi task at the repo root, not bundled into any Python package.
18. As a **developer**, I want the `WorkflowHandler` god-object refactored so that it delegates to narrower interfaces rather than importing eight subpackages directly.
19. As a **data engineer**, I want SV3-specific data models (`sv3_models`, `log_models`, `observables`) to live alongside the Sonardyne parsing code in the same package so they are always in sync.
20. As a **developer**, I want TileDB array schemas and the `TBDArray` base class to live in `earthscope-sfg` so that consumers can use TileDB as a data sink without installing the full workflows package, while workflow-specific TileDB operations (S3 orchestration, tile2rinex, consolidation pipelines) remain in `workflows`.
21. As an **operator**, I want the CLI to support the same `run` and `preprocess` commands with identical flags so that existing automation scripts are unaffected.
22. As a **developer**, I want the Go source code (`golangtools/`) to live inside the `earthscope-sfg` package (under `novatel_tools`) so that Go and Python NovaTel parsing are versioned, tested, and released together.
23. As a **developer**, I want `pixi run build-novatel-go` to cross-compile the Go binaries and place them in a well-known location within the parsing package.
24. As a **data engineer**, I want the parsing package to include all vendor-specific parsing (NovaTel, Sonardyne, PRIDE) so I have one lightweight install for all instrument data.
25. As a **developer**, I want `sonardyne_tools` and `novatel_tools` to coexist in the same package so the Sonardyne QC code can directly import `GNSSEpoch` without requiring a separate package dependency.
26. As a **developer**, I want shared utilities (`command_line_utils`, `custom_warnings_exceptions`, `model_update`) to live in the parsing package alongside the vendor code that uses them, eliminating duplication.
27. As a **developer**, I want the PRIDE-PPPAR Python wrappers (`pride_tools/`) and GNSS product schemas to live in the same package as other parsing code, keeping all data-acquisition concerns together.
28. As a **developer**, I want vendor-specific sub-namespaces (`novatel_tools`, `sonardyne_tools`, `pride_tools`) to remain distinct within the package so the code stays organized by vendor.
29. As a **developer**, I want shared metadata models (site, vessel, campaign, benchmark, catalogs) to live in the `workflows` package since they are orchestration-level concerns used by data_mgmt and modeling.

---

## Implementation Decisions

### Package Layout

The repository will contain three packages under a `packages/` directory, plus the existing root-level pixi workspace:

```
es_sfgtools/                          # repository root
├── pixi.toml                         # workspace-level pixi config
├── packages/
│   ├── earthscope-sfg/
│   │   ├── pyproject.toml
│   │   ├── Makefile                  # Go cross-compilation targets
│   │   ├── src/es_sfgtools/
│   │   │   ├── novatel_tools/        # RangeA parser, RINEX conversion, binary path resolution
│   │   │   ├── sonardyne_tools/      # SV2/SV3 acoustic data parsing, QC
│   │   │   ├── pride_tools/          # PRIDE-PPPAR product retrieval, RINEX utilities
│   │   │   ├── seafloor_site_tools/  # generic SVP processing
│   │   │   ├── tiledb_schemas/       # TileDB array schemas, TBDArray base class + typed subclasses
│   │   │   ├── data_models/          # constants, observables, sv3_models, log_models, community_standards
│   │   │   └── utils/                # command_line_utils, model_update, custom exceptions, rinex_histogram
│   │   └── go/                       # Go source (moved from src/golangtools/)
│   │       ├── go.mod
│   │       ├── go.sum
│   │       ├── cmd/
│   │       │   ├── nova2rnx/         # NovaTel ASCII → RINEX
│   │       │   ├── novb2rnx/         # NovaTel binary → RINEX
│   │       │   ├── nov0002rnx/       # NOV000/0002 → RINEX
│   │       │   ├── nova2tile/        # NovaTel ASCII → TileDB
│   │       │   ├── novab2tile/       # NovaTel binary → TileDB
│   │       │   ├── nov0002tile/      # NOV000/0002 → TileDB
│   │       │   └── tdb2rnx/          # TileDB → RINEX (roundtrip)
│   │       └── pkg/sfg_utils/        # Shared Go parsing library
│   │
│   ├── workflows/
│   │   ├── pyproject.toml
│   │   └── src/es_sfgtools/
│   │       ├── workflows/            # pipelines, preprocess_ingest, midprocess, modeling orchestration
│   │       ├── data_mgmt/            # ingestion, asset catalog, directory management
│   │       ├── modeling/             # garpos_tools, sfgdstf_tools
│   │       ├── prefiltering/         # filter schemas and logic
│   │       ├── tiledb_tools/         # TileDB workflow operations (tile2rinex, S3 config, orchestration)
│   │       ├── config/               # env_config, file_config, garpos_config, loadconfigs, shotdata_filters
│   │       ├── logging/              # ProcessLogger, GarposLogger, PRIDELogger
│   │       └── data_models/          # shared metadata (site, vessel, campaign, benchmark, catalogs)
│   │
│   └── cli/
│       ├── pyproject.toml
│       └── src/es_sfgtools/
│           └── cli/                  # Typer app, manifest models, commands
│
├── external/                         # GARPOS (unchanged), PRIDE-PPPAR (compilation tasks)
├── tests/                            # integration tests (cross-package)
├── dev/                              # dev notebooks & scripts (unchanged)
└── docs/                             # sphinx docs (unchanged)
```

**Note on `data_models/` split:** Both `earthscope-sfg` and `workflows` own a `data_models/` sub-namespace. The parsing package owns vendor-specific data models (constants, observables, sv3_models, log_models, community_standards). The workflows package owns orchestration-level metadata models (site, vessel, campaign, benchmark, catalogs). These are kept in separate sub-directories within `data_models/` to avoid namespace collision — see *Namespace Packaging* below.

### Namespace Packaging

All three packages will use **implicit namespace packages** (PEP 420) under the `es_sfgtools` namespace. Each package owns distinct sub-namespaces:

| Package | Owns namespaces |
|---------|----------------|
| `earthscope-sfg` | `es_sfgtools.novatel_tools`, `es_sfgtools.sonardyne_tools`, `es_sfgtools.pride_tools`, `es_sfgtools.seafloor_site_tools`, `es_sfgtools.tiledb_schemas`, `es_sfgtools.data_models` (vendor-specific: constants, observables, sv3_models, log_models, community_standards), `es_sfgtools.utils` |
| `es-sfgtools-workflows` | `es_sfgtools.workflows`, `es_sfgtools.data_mgmt`, `es_sfgtools.modeling`, `es_sfgtools.prefiltering`, `es_sfgtools.tiledb_tools`, `es_sfgtools.config`, `es_sfgtools.logging`, `es_sfgtools.data_models.metadata` |
| `es-sfgtools-cli` | `es_sfgtools.cli` |

**`data_models` namespace split:** Both `earthscope-sfg` and `workflows` contribute to the `es_sfgtools.data_models` namespace. The parsing package owns top-level data model files (constants, observables, sv3_models, etc.). The workflows package owns the `data_models/metadata/` sub-package (site, vessel, campaign, benchmark, catalogs). Since both use implicit namespace packaging (no `__init__.py` at the `data_models/` level), this works correctly — Python will merge the namespace from both installed packages.

**Critical:** The `es_sfgtools/` directories must NOT contain `__init__.py` files at the namespace level. Each sub-namespace (e.g. `novatel_tools/`) retains its own `__init__.py`.

### Dependency DAG (Strict, No Cycles)

```
es-sfgtools-cli
    └── es-sfgtools-workflows
            └── earthscope-sfg           (no further package deps)
```

- **`earthscope-sfg`** is the foundation package. Depends on lightweight third-party libraries (pydantic, numpy, pandas, pandera, pymap3d, regex) + subprocess (shells out to Go binaries). Contains all vendor-specific parsing (`novatel_tools`, `sonardyne_tools`, `pride_tools`), generic seafloor tools (`seafloor_site_tools`), TileDB array schemas and base classes (`tiledb_schemas`), shared data models (`data_models`), and utilities (`utils`). TileDB is an **optional dependency** (`pip install earthscope-sfg[tiledb]`) — the schemas module requires `tiledb` at import time but the rest of the package does not. No boto3, no GARPOS.
- **`workflows`** depends on `earthscope-sfg[tiledb]` + heavy third-party (boto3, sqlalchemy, gnatss, earthscope-sdk, scikit-sparse). Owns config, logging, orchestration metadata models, prefiltering, tiledb_tools (workflow operations that inherit from `earthscope-sfg`'s base classes), data_mgmt, modeling, and pipeline orchestration.
- **`cli`** depends on `workflows` + typer.

### Go Binary Strategy (`earthscope-sfg` Package)

The Go source code currently at `src/golangtools/` moves into `packages/earthscope-sfg/go/`. The Go module path in `go.mod` will be updated accordingly.

**Build approach:**
- The existing `Makefile` (which cross-compiles for darwin/linux × amd64/arm64) moves to `packages/earthscope-sfg/Makefile`
- A pixi task `build-novatel-go` replaces the current `build-go` task, pointing at the new location
- Compiled binaries are placed in `packages/earthscope-sfg/src/es_sfgtools/novatel_tools/bin/` (same convention as today, where `novatel_tools/utils.py` resolves platform-specific binary paths)
- Binaries are **not** checked into Git — they are built locally via `pixi run build-novatel-go` or in CI
- The `novatel_tools/utils.py` binary resolution logic remains unchanged; it already looks for `{name}_{os}_{arch}` in a `bin/` directory relative to the package

**Why co-locate Go and Python:** NovaTel parsing spans both languages — the Python layer (`rangea_parser.py`) handles pure-Python RANGEA deserialization, while the Go binaries handle high-throughput binary format conversion and TileDB ingestion. They share the same domain, version together, and the Python code shells out to the Go binaries. Keeping them in the same package ensures they are always in sync.

### Config and Logging in Workflows

Since `config/` and `logging/` move to the `workflows` package, they are no longer available to the `earthscope-sfg` parsing package. This is intentional:

- **Parsing code uses Python's stdlib `logging`** module directly, not the custom `ProcessLogger` / `GarposLogger` / `PRIDELogger` wrappers. Those custom loggers are orchestration concerns (structured log routing, file outputs, progress tracking) that belong in workflows.
- **Parsing code does not need `config/`** — `env_config.py` (Environment, AWS credentials, TileDB URIs), `loadconfigs.py` (survey-type dispatch), `garpos_config.py`, `shotdata_filters.py`, and `tiledb_s3_config.py` are all workflow-level orchestration configuration. Vendor parsers are stateless functions that take data in and return structured data out.
- The existing circular dependency (`config/loadconfigs.py` → `prefiltering`) is naturally resolved because both now live in the same `workflows` package.

### Data Model Distribution

Data models are distributed between the two library packages based on domain:

| Current Location | New Location | Rationale |
|---|---|---|
| `data_models/constants.py` (all constants) | `earthscope-sfg: data_models/constants.py` | All constants (GNSS timing, Sonardyne trigger delays, station offsets) stay together in the parsing package |
| `data_models/sv3_models.py` | `earthscope-sfg: data_models/sv3_models.py` | SV3-specific enums and types |
| `data_models/log_models.py` | `earthscope-sfg: data_models/log_models.py` | SV3 interrogation/reply data |
| `data_models/community_standards.py` | `earthscope-sfg: data_models/community_standards.py` | SFGDSTF types used by Sonardyne |
| `data_models/observables.py` | `earthscope-sfg: data_models/observables.py` | ShotDataFrame, AcousticDataFrame, SoundVelocityDataFrame — produced by parsers |
| `data_models/metadata/` (site, vessel, campaign, benchmark, catalogs) | `workflows: data_models/metadata/` | Shared metadata used by data_mgmt, modeling, and orchestration |
| `pride_tools/gnss_product_schemas.py`, `pride_file_config.py`, `rinex_utils.py` | `earthscope-sfg: pride_tools/` (stay in place) | Already scoped to PRIDE; no need to move to a sub-directory since everything is in one package |

### WorkflowHandler Refactoring

The current `WorkflowHandler` is a god-object with direct imports from 8+ subpackages. It will be refactored to:

- Accept injected handler interfaces (DataHandler, PipelineRunner, ModelRunner) rather than constructing them internally
- Each handler interface lives in `workflows` as a Protocol/ABC
- Concrete implementations live alongside in `workflows`
- This decouples the orchestration logic from the concrete implementations

### Versioning Strategy

All packages share **one version** derived from a single Git tag via `setuptools_scm`. Each `pyproject.toml` will use:

```toml
[tool.setuptools_scm]
root = "../.."          # points to the repo root
write_to = "src/es_sfgtools/_version.py"  # in earthscope-sfg package (canonical location)
```

The workflows and cli packages will use their own `setuptools_scm` write target or read the version at build time. Inter-package dependencies will use `==` version pins to enforce co-installation of matching versions.

### Pixi Workspace Configuration

The root `pixi.toml` will be updated to declare the workspace members:

```toml
[workspace]
members = [
  "packages/earthscope-sfg",
  "packages/workflows",
  "packages/cli",
]

[pypi-dependencies]
earthscope-sfg = { path = "packages/earthscope-sfg", editable = true }
es-sfgtools-workflows = { path = "packages/workflows", editable = true }
es-sfgtools-cli = { path = "packages/cli", editable = true }
```

Pixi tasks will be updated:

```toml
[tasks]
build-novatel-go = { cmd = "make -B", cwd = "packages/earthscope-sfg" }
compile-garpos = { cmd = "gfortran -shared ...", cwd = "external/garpos/bin/..." }
clone-pride = { cmd = "...", cwd = "external/" }
install-pride = { cmd = "...", cwd = "external/PRIDE-PPPAR" }
setup = { depends-on = ["compile-garpos", "install-pride", "build-novatel-go"] }
```

### Migration of `app/` to `cli` Package

The current `app/` directory becomes `packages/cli/`. The Typer app, manifest Pydantic models, and command orchestration move into `es_sfgtools.cli`. The `sys.path` hack in `__main__.py` is removed — all imports will go through proper package references.

### Meta-Package

A convenience `es-sfgtools` distribution (the current package name) will be preserved as a thin meta-package that depends on all three sub-packages. Users who `pip install es-sfgtools` get everything, preserving backward compatibility.

```toml
# packages/meta/pyproject.toml (or root pyproject.toml)
[project]
name = "es-sfgtools"
dependencies = [
  "earthscope-sfg",
  "es-sfgtools-workflows",
  "es-sfgtools-cli",
]
```

### External Tools

- **GARPOS** (`external/garpos/`): Remains at the repository root. Not a Python package. Pixi tasks (`compile-garpos`) remain unchanged. The `modeling/garpos_tools/load_utils.py` module (in the `workflows` package) locates binaries via environment variables or well-known paths.
- **PRIDE-PPPAR** (`external/PRIDE-PPPAR/`): The compiled binary remains at the repository root. Pixi tasks (`clone-pride`, `install-pride`) remain unchanged. The `pride_tools/` Python wrapper (in the `pride` package) locates the binary via `PRIDE_PPPAR_BIN` environment variable or a well-known path under the pixi project root.

---

## Testing Decisions

### What Makes a Good Test

Tests should verify **external behavior through public interfaces**, not implementation details. A test for the parsing package should assert "given this RangeA byte string, `deserialize_rangea()` returns a `GNSSEpoch` with these fields" — not "the internal `_parse_header()` function is called with these arguments."

### Per-Package Test Suites

Each package will have its own `tests/` directory:

```
packages/earthscope-sfg/tests/
packages/workflows/tests/
packages/cli/tests/
```

Plus the existing top-level `tests/` for **cross-package integration tests** (e.g., full manifest execution).

### Package-Level Test Mapping

| Package | Test Focus | Prior Art |
|---------|-----------|----------|
| `earthscope-sfg` | RangeA → GNSSEpoch, RINEX generation, Go binary invocation, roundtrip fidelity, GNSS constants, SV3 interrogation/reply parsing, QC JSON processing, acoustic → shot data, SV3 model validation, GNSS product FTP retrieval, RINEX utility functions, product schema validation, SVP parsing, utility functions | `test_rangea_parsing.py`, `test_extract_rangea.py`, `test_rangea_to_rinex.py` |
| `workflows` | Pipeline execution, data ingest, GARPOS integration, TileDB roundtrip, config loading, metadata validation, logging setup | `test_garpos.py`, `test_qc_pipeline.py` |
| `cli` | Manifest parsing, command dispatch, CLI flag handling | New |

### CI Matrix

Each package's tests run in isolation with only that package's dependencies installed (plus its transitive deps). The integration test suite runs with all packages installed.

---

## Out of Scope

- **Documentation restructuring**: Sphinx docs stay at the repo root. Doc generation may need minor import path updates but no structural changes.
- **API changes**: Public function signatures and class interfaces must remain identical. This is a packaging change, not a functional change.
- **Independent versioning**: All packages share one version. Independent release trains are a future consideration.
- **Packaging Go binaries as wheels**: The Go binaries are built locally or in CI via `make`. Distributing them as platform-specific wheel extensions is a separate effort.
- **Removing external/ vendoring**: GARPOS Fortran compilation stays as a pixi task. Packaging it as a wheel is a separate effort.
- **Breaking import paths**: The namespace package structure preserves all existing `from es_sfgtools.X import Y` paths.
- **New features**: This PRD covers only the structural split. No new processing capabilities are added.
- **Rewriting Go code**: The Go source moves directories but is not rewritten or refactored.

---

## Further Notes

### Migration Order

The recommended order of extraction is:

1. **`earthscope-sfg`** — extract `novatel_tools/`, `sonardyne_tools/`, `pride_tools/`, `seafloor_site_tools/`, `data_models/` (vendor-specific files), `utils/`, and TileDB schemas (`tiledb_schemas/` — array schema definitions, `TBDArray` base class, and typed subclasses like `TDBShotDataArray`, `TDBAcousticArray`, etc.) into `packages/earthscope-sfg/`. Move `src/golangtools/` into `packages/earthscope-sfg/go/`. Move the `Makefile` and update pixi task. Data models stay in their current `data_models/` namespace (constants, observables, sv3_models, log_models, community_standards). All internal cross-imports between vendor modules (e.g., `sonardyne_tools` → `novatel_tools`) continue working since they are in the same package.
2. **Workflows** — extract `workflows/`, `data_mgmt/`, `modeling/`, `prefiltering/`, `tiledb_tools/` (workflow-level operations like `tile2rinex`, S3 config, binary path resolution), `config/`, `logging/`, and shared `data_models/metadata/` into `packages/workflows/`. Workflow-specific TileDB array subclasses can inherit from the base classes in `earthscope-sfg`. Declare dependency on `earthscope-sfg[tiledb]`. This is the largest package.
3. **CLI** — move `app/` into `packages/cli/`. Remove the `sys.path` hack. Declare dependency on `es-sfgtools-workflows`.
4. **Meta-package** — create the thin `es-sfgtools` wrapper.

### Risk: `sonardyne_tools` Depends on `novatel_tools`

`sonardyne_tools` imports `GNSSEpoch` from `novatel_tools.rangea_parser` for QC processing. Since both now live in the same `earthscope-sfg` package, this is a simple intra-package import — no cross-package dependency management needed.

### Risk: Go Build Requires CGO + TileDB

The Go binaries that write to TileDB (`nova2tile`, `novab2tile`, `nov0002tile`, `tdb2rnx`) require the TileDB C library at compile time via CGO. The RINEX-only binaries (`nova2rnx`, `novb2rnx`, `nov0002rnx`) do not. Consider splitting the Makefile into two targets:
- `build-novatel-rnx` — RINEX binaries (no CGO, no TileDB dependency)
- `build-novatel-tile` — TileDB binaries (requires CGO + TileDB C headers)

This lets users who only need RINEX conversion avoid the TileDB C dependency entirely.

### Risk: `prefiltering` Straddles Parsing and Workflows

`prefiltering` imports from `data_models`, `logging`, `tiledb_tools`, and `utils`. Since `logging` lives in `workflows`, and `prefiltering` needs the orchestration-level config, `prefiltering` naturally belongs in the `workflows` package. The vendor-specific data models and TileDB base schemas it references from `earthscope-sfg` are available via the `workflows` → `earthscope-sfg` dependency.

### Risk: Namespace Package Gotchas

Implicit namespace packages require that no `__init__.py` exist at the `es_sfgtools/` level in any package. If any package accidentally includes one, it will shadow the namespace for all other packages. The same applies to `data_models/` — since both `earthscope-sfg` and `workflows` contribute to this namespace, neither may have an `__init__.py` at the `data_models/` level. CI should include a check that verifies no `__init__.py` exists at `packages/*/src/es_sfgtools/__init__.py` or `packages/*/src/es_sfgtools/data_models/__init__.py`.

### Risk: `setuptools_scm` with Multiple `pyproject.toml`

Each package's `pyproject.toml` must point `setuptools_scm` at the repository root (`root = "../.."` or `root = "../../.."` depending on depth). Alternatively, a build-time script can inject the version. This needs a spike to validate before implementation begins.

### Risk: Observables Used Across Package Boundaries

`observables.py` (ShotDataFrame, AcousticDataFrame, SoundVelocityDataFrame) lives in the `earthscope-sfg` package alongside the Sonardyne parsing code that produces them and the TileDB schemas that consume them. Since `workflows` depends on `earthscope-sfg`, all workflow modules (tiledb_tools, pipelines) can import these types directly. No cross-package boundary issues.

### Risk: TileDB as Optional Dependency

The `tiledb_schemas` module in `earthscope-sfg` requires the `tiledb` Python package at import time. To keep the base install lightweight for users who only need parsing, `tiledb` is declared as an optional dependency (`[project.optional-dependencies] tiledb = ["tiledb"]`). The `workflows` package depends on `earthscope-sfg[tiledb]` to ensure TileDB is always available in workflow contexts. Users who want TileDB array support without workflows install `pip install earthscope-sfg[tiledb]`. The `tiledb_schemas` module should guard the `import tiledb` with a clear error message if the extra is not installed.

### Risk: Parsing Package Losing Custom Loggers

Moving `logging/` to `workflows` means vendor parsing code in `earthscope-sfg` (novatel, sonardyne, pride) can no longer import `ProcessLogger` or `PRIDELogger`. Parsing code must be migrated to use Python's stdlib `logging` module. The custom loggers in `workflows` can still be configured at the orchestration level to capture and format logs from parsing code via standard logging handlers. This is a cleaner separation — parsers emit structured log messages, and the workflow layer decides how to route them.

### Risk: `constants.py` Stays Unified

With all vendor parsing in a single `earthscope-sfg` package, `data_models/constants.py` no longer needs to be split. GNSS timing constants and Sonardyne instrument constants can coexist in the same file since both consumers are in the same package. This is simpler than the previous design which required splitting constants across packages.

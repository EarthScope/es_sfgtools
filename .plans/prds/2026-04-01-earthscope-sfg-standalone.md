# PRD: Standalone `earthscope-sfg` Package

**Date:** 2026-04-01
**Authors:** Franklyn Dunbar
**Status:** Draft
**Related:** [Mono-Repository Package Split PRD](2026-03-31-mono-repo-package-split.md)

---

## Problem Statement

The `earthscope-sfg` package already exists as a directory under `packages/earthscope-sfg/` with its own `pyproject.toml`, but it is not yet a fully standalone, independently publishable package. Several issues prevent a clean `pip install earthscope-sfg` experience today:

1. **No `__init__.py` for `data_models/`** — The `data_models` namespace is a PEP 420 implicit namespace package shared between `earthscope-sfg` (constants, observables, sv3_models, log_models, community_standards) and `workflows` (metadata/). This works inside the monorepo's editable install but is fragile for standalone PyPI distribution. A user who installs only `earthscope-sfg` gets `es_sfgtools.data_models.constants` but has no package-level `__init__.py` — IDE tooling, auto-discovery, and `help()` suffer.

2. **Ghost `pride_tools` package** — `pyproject.toml` declares `es_sfgtools.pride_tools` but the directory `packages/earthscope-sfg/src/pride_tools/` does not exist (it was deleted as dead code). The build will fail or produce an empty namespace.

3. **Go binaries have no distribution strategy** — The 8 Go CLI binaries (`nova2rnx`, `novb2rnx`, `nov0002rnx`, `nova2tile`, `novab2tile`, `nov0002tile`, `tdb2rnx`, `rnxqc`) are built locally via `make` and resolved at runtime by platform. There is no `setup.py`/wheel hook to bundle them, no CI matrix to cross-compile, and no graceful degradation when they are absent.

4. **Missing test infrastructure** — There are no standalone tests for `earthscope-sfg`. The existing `tests/` directory at the repo root contains integration tests that depend on `workflows` (e.g., `test_rangea_parsing.py` calls `tile2rinex` from `tiledb_tools`). A standalone package needs its own test suite that validates parsing without importing workflow code.

5. **No standalone documentation** — The Sphinx docs at `docs/` document the entire monorepo. There is no `earthscope-sfg`-specific README, API reference, or quick-start guide for users who install only the parsing package.

6. **`community_standards.py` imports from `metadata`** — The `community_standards.py` module in `earthscope-sfg` imports `from .metadata import Site, Vessel`, which is a cross-package dependency on `workflows`' `data_models/metadata/` sub-package. This breaks standalone installation.

7. **No CI/CD for independent release** — The package cannot be built, tested, or published independently. There is no GitHub Actions workflow for `earthscope-sfg` alone.

8. **`utils/model_update.py` pulls in pydantic BaseSettings** — The `validate_and_merge_config` function is used by both `earthscope-sfg` consumers (CLI, workflows) and internally. Its signature and behavior need to be stable and documented for external users.

---

## Solution

Make `earthscope-sfg` a fully standalone, independently installable and publishable Python package on PyPI that provides all vendor-specific seafloor geodesy parsing functionality. The package will:

- Parse NovaTel GNSS receiver logs (RANGEA binary, ASCII, NOV000) → RINEX
- Parse Sonardyne SV3 acoustic positioning data → ShotData, AcousticData
- Provide GNSS-A community standard data schemas (SFGDSTF)
- Provide TileDB array schemas for seafloor geodesy data types (optional `[tiledb]` extra)
- Provide Go CLI binaries for high-throughput conversion (optional `[go-binaries]` extra or separate install step)
- Ship with sound velocity profile processing utilities
- Expose a stable, documented public API under the `es_sfgtools.*` namespace
- Include its own test suite, README, and API documentation
- Be publishable to PyPI via a dedicated CI workflow

The package remains in the monorepo and continues to be developed alongside `workflows` and `cli`, but it can be installed, tested, and released independently.

---

## User Stories

1. As a **data engineer**, I want to `pip install earthscope-sfg` and immediately parse NovaTel RangeA logs to RINEX without installing TileDB, boto3, GARPOS, or any workflow dependencies.
2. As a **data engineer**, I want to parse Sonardyne SV3 acoustic data into validated `ShotDataFrame` and `AcousticDataFrame` objects using only the standalone package.
3. As a **scientist**, I want to import `from es_sfgtools.novatel_tools import deserialize_rangea` in a standalone script without needing the full monorepo installed.
4. As a **scientist**, I want to validate my acoustic data against Pandera schemas (`ShotDataFrame`, `AcousticDataFrame`) without installing orchestration dependencies.
5. As a **scientist**, I want to convert my data to the GNSS-A community standard format (`SFGDSTFSeafloorAcousticData`) using only the parsing package.
6. As a **data engineer**, I want `pip install earthscope-sfg[tiledb]` to additionally provide TileDB array schemas and typed array classes so I can write parsed data directly to TileDB without the workflows package.
7. As a **data engineer**, I want the Go binaries (`nova2rnx`, `novb2rnx`, etc.) to be optional — pure Python parsing works without them, and I can install or build them separately when I need high-throughput conversion.
8. As a **developer**, I want to run `pytest` inside `packages/earthscope-sfg/` and get a full pass without installing `workflows` or `cli`.
9. As a **developer**, I want a clear `README.md` in `packages/earthscope-sfg/` that documents installation, quick-start usage, and the public API surface.
10. As a **developer**, I want `earthscope-sfg` to have zero imports from `es_sfgtools.workflows`, `es_sfgtools.config`, `es_sfgtools.data_mgmt`, `es_sfgtools.modeling`, `es_sfgtools.prefiltering`, `es_sfgtools.tiledb_tools`, `es_sfgtools.logging`, or `es_sfgtools.data_models.metadata` — verified by CI.
11. As a **CI system**, I want a GitHub Actions workflow that builds, tests, and optionally publishes `earthscope-sfg` independently of workflows and CLI.
12. As a **release manager**, I want `earthscope-sfg` to use the same `setuptools_scm` version derived from Git tags so that version compatibility between monorepo packages is trivially verifiable.
13. As a **developer**, I want `pyproject.toml` to accurately reflect the package contents — no ghost packages like `pride_tools` that have no corresponding directory.
14. As a **developer**, I want the `community_standards.py` module to not import from `data_models.metadata` (which lives in workflows), so that the package is truly standalone.
15. As a **data engineer**, I want clear error messages when I try to use a Go binary that is not installed, with instructions on how to build or obtain it.
16. As a **developer**, I want the public API of each sub-module (`novatel_tools`, `sonardyne_tools`, `data_models`, `tiledb_schemas`, `utils`) to be explicitly declared in `__init__.py` files with `__all__` exports.
17. As a **data engineer**, I want the package to install cleanly in a fresh virtualenv on macOS (arm64/x86), Linux (x86/arm64), and Windows (x86) with `pip install earthscope-sfg`.
18. As a **scientist**, I want example scripts showing common parsing workflows (RangeA → RINEX, SV3 JSON → ShotData, ShotData → community standard format).
19. As a **developer**, I want type stubs or inline type annotations for the public API so that mypy/pyright provide useful completions and error checking.
20. As a **data engineer**, I want the sound velocity profile utilities (`seafloor_site_tools`) to be available as part of the package for correcting acoustic travel times.
21. As a **developer**, I want the `data_models/` directory to have a proper `__init__.py` that re-exports the public API (DataFrames, constants, enums) for discoverability, while still functioning as a namespace package when `workflows` is also installed.
22. As a **operator**, I want `python -m es_sfgtools.novatel_tools` or a CLI entry point (e.g., `sfg-parse`) that exposes basic parsing commands without requiring the full CLI package.
23. As a **developer**, I want a `CHANGELOG.md` or release notes strategy for the standalone package so users can track breaking changes.
24. As a **CI system**, I want an import-isolation test that verifies `earthscope-sfg` can be imported in an environment where `workflows` is NOT installed.

---

## Implementation Decisions

### Package Identity

- **PyPI name:** `earthscope-sfg`
- **Python namespace:** `es_sfgtools.*` (unchanged — namespace package under shared `es_sfgtools` root)
- **Owned sub-namespaces:** `novatel_tools`, `sonardyne_tools`, `seafloor_site_tools`, `tiledb_schemas`, `data_models` (top-level files only), `utils`
- **No `__init__.py` at `es_sfgtools/` level** — implicit namespace package (PEP 420) shared with `workflows` and `cli`

### Fix Ghost `pride_tools` Package

The `pyproject.toml` currently declares `es_sfgtools.pride_tools` but the directory was deleted. Two options:
- **Option A (recommended):** Remove `es_sfgtools.pride_tools` from `pyproject.toml`'s `packages` list. The PRIDE-PPPAR wrapper code was removed as dead code. If it is resurrected later, it gets re-added.
- **Option B:** Create a stub `pride_tools/` directory with an `__init__.py` that raises `ImportError("pride_tools has been removed")`.

Decision: **Option A** — clean removal.

### Fix `community_standards.py` Cross-Package Import

`community_standards.py` currently imports `from .metadata import Site, Vessel`. The `metadata` sub-package lives in `workflows`, not `earthscope-sfg`. For standalone use, this import will fail.

Resolution: Make `Site` and `Vessel` references in `community_standards.py` optional or restructure the function signatures:
- The `SFGDSTFSiteMetaData` model (which uses `Site` and `Vessel`) will accept raw dictionaries or simple dataclasses instead of importing the full Pydantic metadata models from workflows.
- Alternatively, extract the minimal `Site`/`Vessel` field definitions needed by `community_standards.py` into a lightweight protocol or TypedDict in `earthscope-sfg`'s own `data_models/` directory, and have `workflows`' full Pydantic models satisfy that protocol.

### `data_models/` Package Structure

The `data_models/` directory needs a `__init__.py` to provide discoverability when installed standalone. However, when both `earthscope-sfg` and `workflows` are installed (the monorepo case), having `__init__.py` at the `data_models/` level would break the implicit namespace merge.

Resolution: **Conditional `__init__.py`** approach:
- Add a `data_models/__init__.py` that re-exports from the top-level modules (constants, observables, sv3_models, log_models).
- When `workflows` is also installed, Python's namespace packaging will still merge `data_models.metadata` from the workflows package. The presence of `__init__.py` means `data_models` becomes a regular package owned by `earthscope-sfg`, and workflows must use `es_sfgtools.data_models.metadata` as a separate namespace entry point.
- **Alternative (simpler):** Do NOT add `__init__.py`. Users import directly: `from es_sfgtools.data_models.observables import ShotDataFrame`. This is the current pattern and works fine.

Decision: **No `__init__.py` at `data_models/` level.** Keep it as an implicit namespace. Document the direct-import pattern in the README. This avoids the regular-package-vs-namespace conflict entirely.

### Go Binary Distribution Strategy

Go binaries are **optional**. The resolution strategy:

1. **Runtime detection:** `get_binary_path()` (in `utils/command_line_utils.py`) already resolves platform-specific binaries. When a binary is not found, it should raise a clear `FileNotFoundError` with a message like: `"Go binary 'nova2rnx' not found. Build with 'make -C <path>' or install from <release_url>"`.
2. **Local build:** The existing `Makefile` in `packages/earthscope-sfg/go/` cross-compiles all binaries. Document `make -C packages/earthscope-sfg/go/ all` in the README.
3. **CI build (future):** A GitHub Actions workflow will cross-compile binaries and attach them as release artifacts. Users can download platform-specific binaries and place them in the expected location.
4. **No wheel bundling (initial release):** Bundling binaries into platform-specific wheels adds significant CI complexity (cibuildwheel, manylinux, etc.). Defer this to a future iteration. The initial release is pure Python + optional manual binary install.
5. **Python-only fallback:** The pure-Python RangeA parser (`rangea_parser.py`) and all Sonardyne parsing work without Go binaries. Only the high-throughput NovaTel conversion pipelines and TileDB-to-RINEX roundtrip require Go.

### TileDB as Optional Extra

TileDB remains an optional extra: `pip install earthscope-sfg[tiledb]`.

- The `tiledb_schemas/` module requires `tiledb` at import time and uses a try/except guard in `__init__.py`.
- The `arrays.py` module uses `cloudpathlib` for S3 URI support.
- When `tiledb` is not installed, importing `es_sfgtools.tiledb_schemas` raises a clear `ImportError` explaining how to install the extra.
- All other modules (`novatel_tools`, `sonardyne_tools`, `data_models`, `utils`, `seafloor_site_tools`) work without `tiledb`.

### Public API Surface

Each sub-module must have explicit `__init__.py` exports. The public API is:

**`es_sfgtools.novatel_tools`:**
- `novatel_ascii_2rinex()`, `novatel_2rinex()` — RINEX conversion
- `deserialize_rangea()`, `extract_rangea_from_qcpin()` — RangeA parsing
- `GNSSEpoch`, `Satellite`, `Observation`, `GNSSSystem` — data models
- `MetadataModel` — metadata container
- Binary path helpers (internal, not public API)

**`es_sfgtools.sonardyne_tools`:**
- `dfop00_to_shotdata()`, `dfop00_to_SFGDSTFSeafloorAcousticData()` — SV3 parsing
- `merge_interrogation_reply()` — data merging
- `qcjson_to_shotdata()`, `batch_qc_by_day()` — QC processing
- `novatelInterrogation_to_garpos_interrogation()`, `novatelReply_to_garpos_reply()` — GARPOS format

**`es_sfgtools.data_models.observables`:**
- `AcousticDataFrame`, `GNSSObsDataFrame`, `IMUPositionDataFrame`, `KinPositionDataFrame`, `ShotDataFrame`, `SoundVelocityDataFrame` — Pandera validation schemas

**`es_sfgtools.data_models.constants`:**
- `GNSS_START_TIME`, `LEAP_SECONDS`, `TRIGGER_DELAY_SV2`, `TRIGGER_DELAY_SV3`, `STATION_OFFSETS`, `MASTER_STATION_ID`

**`es_sfgtools.data_models.sv3_models`:**
- `SV3GPSQuality`, `NovatelSolutionStatus`, `NovatelPositionType` — enums

**`es_sfgtools.data_models.log_models`:**
- `SV3InterrogationData`, `SV3ReplyData` — Pydantic models

**`es_sfgtools.data_models.community_standards`:**
- `SFGDSTFSeafloorAcousticData` — community standard schema (with `Site`/`Vessel` dependency resolved)

**`es_sfgtools.tiledb_schemas` (requires `[tiledb]` extra):**
- `AcousticArraySchema`, `GNSSObsSchema`, `IMUPositionArraySchema`, `KinPositionArraySchema`, `ShotDataArraySchema`
- `TBDArray`, `TDBAcousticArray`, `TDBGNSSObsArray`, `TDBIMUPositionArray`, `TDBKinPositionArray`, `TDBShotDataArray`

**`es_sfgtools.utils`:**
- `validate_and_merge_config()` — config merging utility
- `get_binary_path()`, `run_binary()` — binary execution helpers

**`es_sfgtools.seafloor_site_tools`:**
- Sound velocity profile processing (exact API TBD from `soundspeed_operations.py`)

### Test Strategy

A new `packages/earthscope-sfg/tests/` directory with:

1. **Unit tests for each parser module:**
   - `test_rangea_parser.py` — RangeA deserialization with fixture data
   - `test_novatel_ascii.py` — ASCII RINEX conversion
   - `test_sv3_operations.py` — SV3 parsing and QC
   - `test_soundspeed.py` — Sound velocity profile processing

2. **Schema validation tests:**
   - `test_observables.py` — Pandera schema acceptance/rejection
   - `test_data_models.py` — Pydantic model serialization round-trips

3. **Import isolation test:**
   - `test_standalone_imports.py` — Verifies all public modules import without `workflows` installed (run in a clean venv in CI)

4. **Binary path resolution tests:**
   - `test_binary_utils.py` — Tests `get_binary_path()` with mocked file system

### CI/CD

A GitHub Actions workflow (`earthscope-sfg-ci.yml`) that:

1. Installs only `earthscope-sfg` in a clean venv (no monorepo editable install)
2. Runs `packages/earthscope-sfg/tests/`
3. Verifies zero imports from `workflows` namespace (`grep -r "from es_sfgtools.workflows\|from es_sfgtools.config\|from es_sfgtools.data_mgmt" packages/earthscope-sfg/src/`)
4. Builds the sdist and wheel
5. Optionally publishes to PyPI on tagged releases

### `pyproject.toml` Corrections

The existing `pyproject.toml` needs these fixes:
- Remove `es_sfgtools.pride_tools` from `packages` list (directory deleted)
- Add `es_sfgtools.seafloor_site_tools` if not already present
- Ensure `package-data` includes Go binary paths for optional binary bundling
- Add `[project.urls]` for Documentation, Repository, Changelog
- Add `[project.classifiers]` for PyPI metadata

### README Structure

A standalone `packages/earthscope-sfg/README.md` covering:
- One-line description and badges
- Installation (`pip install`, optional extras)
- Quick-start examples (RangeA → RINEX, SV3 → ShotData)
- Go binary setup (optional)
- API overview (modules and key functions)
- Link to full documentation
- Contributing guide reference

---

## Testing Decisions

### What Makes a Good Test

Tests should verify **external behavior** (inputs → outputs) of the public API, not internal implementation details. A good test for this package:
- Provides a fixture file (real or synthetic instrument data)
- Calls a public API function
- Asserts on the shape, types, and values of the output
- Does not mock internal helpers unless testing error paths
- Does not import from `workflows` or any other monorepo package

### Modules to Test

| Module | Test Type | Priority |
|--------|-----------|----------|
| `novatel_tools.rangea_parser` | Unit — fixture-based parsing | High |
| `novatel_tools.novatel_ascii_operations` | Unit — ASCII conversion | High |
| `sonardyne_tools.sv3_operations` | Unit — SV3 parsing | High |
| `sonardyne_tools.sv3_qc_operations` | Unit — QC JSON processing | High |
| `data_models.observables` | Schema — acceptance/rejection | High |
| `data_models.sv3_models` | Unit — enum values, model validation | Medium |
| `data_models.log_models` | Unit — serialization round-trip | Medium |
| `tiledb_schemas` | Unit — schema construction (if tiledb installed) | Medium |
| `utils.command_line_utils` | Unit — binary path resolution, log parsing | Medium |
| `utils.model_update` | Unit — config merge logic | Medium |
| `seafloor_site_tools.soundspeed_operations` | Unit — SVP corrections | Low |
| Standalone import isolation | Integration — clean venv import check | High |

### Prior Art

The existing `tests/` directory contains:
- `test_extract_rangea.py` — Tests `extract_rangea_from_qcpin` (pattern to follow, but currently broken)
- `test_qc_pipeline.py` — QC pipeline test (uses workflows, not applicable for standalone)
- `tests/resources/sv3/` — SV3 test fixture files (can be reused)
- `tests/resources/qcdata/` — QC JSON test data (can be reused)

---

## Out of Scope

1. **Refactoring `workflows` package** — This PRD is about making `earthscope-sfg` standalone. Changes to workflows are limited to removing reverse dependencies (if any are discovered).
2. **Publishing to conda-forge** — Initial release targets PyPI only. Conda-forge recipe is a future follow-up.
3. **Platform-specific wheel builds** — Go binary bundling in wheels is deferred. Initial release is pure Python with optional manual binary install.
4. **Independent versioning** — All packages continue to share a single Git-tag-derived version via `setuptools_scm`. Independent semver for `earthscope-sfg` alone is a future consideration.
5. **PRIDE-PPPAR tools** — The `pride_tools` module was removed. If resurrected, it will be a separate PRD.
6. **Windows support for Go binaries** — Go cross-compilation targets macOS and Linux only. Windows binary builds are out of scope for this release.
7. **Deprecation of old import paths** — No import paths change. The `es_sfgtools.*` namespace is preserved.
8. **CLI entry points for parsing** — A `sfg-parse` CLI for standalone parsing is a nice-to-have but not required for the initial release.

---

## Further Notes

### Relationship to Mono-Repo Split PRD

This PRD is a tactical subset of the [Mono-Repository Package Split PRD](2026-03-31-mono-repo-package-split.md). The split PRD defines the high-level architecture for three packages. This PRD focuses exclusively on making `earthscope-sfg` independently installable and publishable as the **first concrete deliverable** of that split.

### Namespace Package Considerations

The shared `es_sfgtools` namespace means that when both `earthscope-sfg` and `workflows` are installed, Python merges their namespace contributions. Key constraints:
- No `__init__.py` at the `es_sfgtools/` level in any package
- No `__init__.py` at the `es_sfgtools/data_models/` level (shared namespace between packages)
- Each sub-package (`novatel_tools/`, `sonardyne_tools/`, etc.) has its own `__init__.py`

When only `earthscope-sfg` is installed, `es_sfgtools.data_models.metadata` will not be available (it's provided by `workflows`). Code that attempts to import it will get a clear `ModuleNotFoundError`.

### Migration Path for Existing Users

Users currently consuming the monorepo via `pip install -e .` at the repo root will:
1. Continue to work unchanged (pixi workspace installs all packages in editable mode)
2. Gradually migrate to importing only from the sub-namespaces they need
3. When `earthscope-sfg` is published to PyPI, downstream users who only need parsing can switch to `pip install earthscope-sfg`

### Sound Speed Dependency

The `seafloor_site_tools/soundspeed_operations.py` module may have hidden dependencies on scipy/numpy beyond what's already declared. Audit during implementation.

### Decimal Precision

Several data models (`log_models.py`, `sv3_models.py`) use `decimal.Decimal` with `getcontext().prec = 10`. This global state mutation should be documented as a known side-effect of importing these modules, and potentially refactored to use a module-local context in a follow-up.

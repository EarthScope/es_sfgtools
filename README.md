# EarthScope Seafloor Geodesy Tools
[![Read the Docs](https://readthedocs.org/projects/es-sfgtools/badge/?version=latest)](https://es-sfgtools.readthedocs.io/en/latest/)

`es_sfgtools` is a Python library designed to support preprocessing and GNSS-A processing workflows for Seafloor Geodesy using data from Liquid Robotics SV2/SV3 Wave Gliders.

The toolkit also integrates with the [**GARPOS**](https://github.com/s-watanabe-jhod/garpos) GNSS-A processing.

## Monorepo Structure

This repository is organized as a monorepo with the following packages:

| Package | Path | Description |
|---------|------|-------------|
| **earthscope-sfg** | `packages/earthscope-sfg/` | Core parsing library — NovaTel, Sonardyne, data models, TileDB schemas |
| **es-sfgtools-workflows** | `packages/earthscope-sfg-workflows/` | Workflow orchestration — pipelines, data management, modeling |
| **es-sfgtools-cli** | `packages/cli/` | CLI for manifest-driven pipeline execution |

All three packages share the `es_sfgtools` namespace and are installed together via [pixi](https://pixi.sh).

## Installation

### Prerequisites

- [pixi](https://pixi.sh) (recommended) or conda/mamba
- gfortran (for GARPOS — on macOS: `brew install gfortran`)

### Quick Start

```bash
git clone https://github.com/EarthScope/es_sfgtools.git
cd es_sfgtools

# Install environment and all packages
pixi install

# Build external dependencies (GARPOS, PRIDE-PPPAR, Go binaries)
pixi run setup

# Verify the setup
pixi run test-setup
```

### Development

```bash
# Lint and format
pixi run lint
pixi run format

# Run tests
pixi run pytest tests/ -v

# Build documentation
pixi run docs
```

## Documentation

Documentation (in development) is available on ReadTheDocs:

[ReadTheDocs](https://es-sfgtools.readthedocs.io/en/latest/)

---

**Maintainers**: Mike Gottlieb, Franklyn Dunbar, Rachel Akie
**Organization**: [EarthScope](https://www.earthscope.org/)
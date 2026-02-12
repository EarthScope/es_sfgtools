# Pixi Workspace Setup Guide

This guide walks you through setting up the `es_sfgtools` development environment using [Pixi](https://pixi.sh).

## Step 1: Install Pixi

### macOS / Linux

```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

### Windows (PowerShell)

```powershell
iwr -useb https://pixi.sh/install.ps1 | iex
```

### Verify Installation

```bash
pixi --version
```

---

## Step 2: Clone the Repository

```bash
git clone https://github.com/EarthScope/es_sfgtools.git
cd es_sfgtools
```

---

## Step 3: Install the Default Workspace

This installs Python, conda dependencies, and PyPI packages defined in `pixi.toml` and `pyproject.toml`:

```bash
pixi install
```

For the full development environment (includes dev tools + Jupyter):

```bash
pixi install -e full
```

---

## Step 4: Setup External Dependencies

Run the setup task to clone and compile external tools (GARPOS, PRIDE-PPPAR):

```bash
pixi run setup
```

This will:
- Clone and compile GARPOS (Fortran raytracing library)
- Clone and install PRIDE-PPPAR (GNSS processing)
- Compile golang binaries

---

## Step 5: Test dependencies (optional)

Run the test task to check pride and garpos installs

```bash
pixi run test-setup
```

This will:
- Test the GARPOS install
- Test the PRIDE-PPPAR install


## Step 6: Enter the Workspace Shell

Activate the pixi environment shell:

```bash
pixi shell
```

You are now inside the workspace with:
- All dependencies available
- Environment variables configured (`GARPOS_PATH`, `PATH` includes PRIDE binaries, etc.)
- Python and all packages ready to use

To exit the shell:

```bash
exit
```

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `pixi install` | Install default environment |
| `pixi install -e dev` | Install with dev tools |
| `pixi install -e full` | Install with dev + notebook |
| `pixi run setup` | Setup external dependencies |
| `pixi shell` | Enter the workspace shell |
| `pixi run <task>` | Run a task without entering shell |
| `pixi run test-setup` | Run tests |
| `pixi run build-go` | Build Go tools |
| `pixi clean` | Remove installed environments |

---

## Available Environments

| Environment | Features |
|-------------|----------|
| `default` | Core runtime dependencies |
| `dev` | + black, pytest, sphinx, docs tools |
| `notebook` | + ipykernel, jupyterlab |
| `full` | All of the above |

---

## Troubleshooting

### Pixi command not found

Make sure pixi is in your PATH. You may need to restart your terminal or add to your shell profile:

```bash
# bash
echo 'export PATH="$HOME/.pixi/bin:$PATH"' >> ~/.bashrc

# zsh
echo 'export PATH="$HOME/.pixi/bin:$PATH"' >> ~/.zshrc
```

### Clean reinstall

```bash
pixi clean
pixi install
pixi run setup
```

### Check environment variables

```bash
pixi shell
echo $GARPOS_PATH
echo $PATH | tr ':' '\n' | grep PRIDE
```

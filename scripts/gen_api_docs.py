#!/usr/bin/env python3
"""
Generate Sphinx API rst files for all three monorepo packages.

Discovers packages/modules under each src/ directory and writes
correctly-namespaced automodule rst files into docs/source/api/.

Usage:
    pixi run python scripts/gen_api_docs.py
"""

import shutil
from pathlib import Path

ROOT = Path(__file__).parent.parent
API_DIR = ROOT / "docs" / "source" / "api"

# (namespace_root, src_directory)
PACKAGES = [
    ("earthscope_sfg", ROOT / "packages" / "earthscope-sfg" / "src"),
    ("earthscope_sfg_workflows", ROOT / "packages" / "earthscope-sfg-workflows" / "src"),
    ("earthscope_sfg_cli", ROOT / "packages" / "cli" / "src"),
]


def _underline(text: str, char: str = "=") -> str:
    return char * len(text)


def _module_rst(module_path: str) -> str:
    title = f"{module_path} module"
    return f"""{title}
{_underline(title)}

.. automodule:: {module_path}
   :members:
   :undoc-members:
   :show-inheritance:
"""


def _package_rst(pkg_path: str, children: list[str]) -> str:
    title = f"{pkg_path} package"
    content = f"""{title}
{_underline(title)}

.. automodule:: {pkg_path}
   :members:
   :undoc-members:
   :show-inheritance:
"""
    if children:
        content += """
Submodules
----------

.. toctree::
   :maxdepth: 4

"""
        for child in sorted(children):
            content += f"   {child}\n"
    return content


def collect(src_dir: Path, namespace: str) -> list[tuple[str, str]]:
    """Return list of (filename, content) for all rst files under namespace."""
    results: list[tuple[str, str]] = []

    def walk(directory: Path, pkg_prefix: str) -> None:
        if not (directory / "__init__.py").exists():
            return

        children: list[str] = []

        for item in sorted(directory.iterdir()):
            if item.is_dir() and not item.name.startswith("_"):
                child_pkg = f"{pkg_prefix}.{item.name}"
                if (item / "__init__.py").exists():
                    children.append(child_pkg)
                    walk(item, child_pkg)

            elif item.is_file() and item.suffix == ".py" and not item.name.startswith("_"):
                mod_name = f"{pkg_prefix}.{item.stem}"
                children.append(mod_name)
                results.append((f"{mod_name}.rst", _module_rst(mod_name)))

        results.append((f"{pkg_prefix}.rst", _package_rst(pkg_prefix, children)))

    for top_dir in sorted(src_dir.iterdir()):
        if top_dir.is_dir() and not top_dir.name.startswith("_"):
            if (top_dir / "__init__.py").exists():
                walk(top_dir, f"{namespace}.{top_dir.name}")
            # For flat src/ layout (cli package): modules directly in src/
            elif top_dir.suffix == "" and top_dir.name not in ("__pycache__",):
                pass  # skip non-package dirs

    # Handle flat modules directly in src/ (e.g. cli's __main__, commands, etc.)
    for f in sorted(src_dir.iterdir()):
        if f.is_file() and f.suffix == ".py" and not f.name.startswith("_"):
            mod_name = f"{namespace}.{f.stem}"
            results.append((f"{mod_name}.rst", _module_rst(mod_name)))

    return results


def main() -> None:
    # Wipe all existing api/ rst files
    for old in API_DIR.glob("*.rst"):
        old.unlink()
    print(f"Cleared {API_DIR}")

    total = 0
    for namespace, src_dir in PACKAGES:
        files = collect(src_dir, namespace)
        for filename, content in files:
            (API_DIR / filename).write_text(content)
            total += 1
        print(f"  {namespace}: {len(files)} files")

    print(f"Generated {total} rst files in {API_DIR}")


if __name__ == "__main__":
    main()

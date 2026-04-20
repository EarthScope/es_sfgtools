"""
Sphinx configuration for the es-sfgtools monorepo documentation.
"""

import os
import sys

# ---------------------------------------------------------------------------
# sys.path — add all three package src directories so autodoc can import
# modules even when the packages are not installed (e.g. local builds).
# When installed (pixi env / ReadTheDocs), these paths are harmless duplicates.
# ---------------------------------------------------------------------------
_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for _pkg_src in [
    "packages/earthscope-sfg/src",
    "packages/earthscope-sfg-workflows/src",
    "packages/cli/src",
]:
    _p = os.path.join(_root, _pkg_src)
    if _p not in sys.path:
        sys.path.insert(0, _p)

# -- Project information -----------------------------------------------------

project = "es-sfgtools"
copyright = "2024, EarthScope"
author = "EarthScope"

try:
    from importlib.metadata import version

    release = version("earthscope-sfg")
except Exception:
    release = "0.0.0"

version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "myst_parser",
]

napoleon_numpy_docstring = True
myst_enable_extensions = ["colon_fence"]

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

# Packages that may not be present during doc builds
autodoc_mock_imports = [
    "georinex",
    "tiledb",
    "boto3",
    "botocore",
    "sqlalchemy",
    "earthscope_sdk",
    "earthscope_cli",
    "gnatss",
    "cloudpathlib",
    "seaborn",
    "pride_ppp",
    "garpos",
    "pyarrow",
    "pandera",
    "ctypes",
]

templates_path = ["_templates"]
exclude_patterns = []

# -- HTML output -------------------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_logo = "./_static/EarthScope_Logo-color.png"
html_theme_options = {
    "collapse_navigation": False,
}

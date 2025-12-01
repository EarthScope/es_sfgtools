"""
This module contains the configuration for the Sphinx documentation builder.
"""
# Configuration file for the Sphinx documentation builder.
import os
import sys

# sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('../../src/es_sfgtools'))

# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'es-sfgtools'
copyright = '2024, EarthScope'
author = 'EarthScope'

# Get version from setuptools_scm
try:
    from importlib.metadata import version
    release = version('es-sfgtools')
except Exception:
    release = '0.0.0'

version = '.'.join(release.split('.')[:2])  # Short X.Y version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.napoleon',        # support for numpy-style docstrings
              'sphinx.ext.autodoc',         # support for automatic documentation
              'sphinx.ext.autosummary',     # support for automatic summaries
              'sphinx.ext.duration',        # support for duration format
              'sphinx.ext.doctest',         # support for doctests
              'sphinxcontrib.apidoc',       # support for automatic API documentation
              'myst_parser'                 # support for markdown
              ]


napoleon_numpy_docstring = True
myst_enable_extensions = ["colon_fence"]    # Enables the use of ::: for fenced code blocks.

# APIDOC configuration 
apidoc_module_dir = "../../src/es_sfgtools"
apidoc_output_dir = "api"
apidoc_separate_modules = True
apidoc_toc_file = False
apidoc_module_first = True

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_logo = "./_static/EarthScope_Logo-color.png"
html_theme_options = {
    'collapse_navigation': False,
    # Other options...
}

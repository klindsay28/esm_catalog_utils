# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os.path
import sys

sys.path.insert(0, os.path.abspath("../esm_catalog_utils/"))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "esm_catalog_utils"
copyright = "2023"
author = "@klindsay28"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "myst_nb",
]

templates_path = ["_templates"]
exclude_patterns = []

autosummary_generate = True
autodoc_typehints = "none"

nb_execution_mode = "off"

intersphinx_mapping = {
    "intake-esm": ("https://intake-esm.readthedocs.io/en/stable/", None),
    "dask": ("https://docs.dask.org/en/stable/", None),
    "dask.distributed": ("https://distributed.dask.org/en/stable/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "zarr": ("https://zarr.readthedocs.io/en/stable/", None),
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"

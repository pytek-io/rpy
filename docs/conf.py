#!/usr/bin/env python3
from __future__ import annotations
import sys

if sys.version_info >= (3, 8):
    from importlib.metadata import version as get_version

from packaging.version import parse

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
]

templates_path = ["_templates"]
source_suffix = ".rst"
master_doc = "index"
project = "RMY"
author = "Francois du Vignaud"
copyright = "2023, " + author

if sys.version_info >= (3, 8):
    v = parse(get_version("rmy"))
    version = v.base_version
    release = v.public
else:
    import rmy

    version = rmy.__version__
    release = rmy.__version__

language = "en"

exclude_patterns = ["_build"]
pygments_style = "sphinx"
autodoc_default_options = {"members": True, "show-inheritance": True}
todo_include_todos = False

html_theme = "sphinx_rtd_theme"
htmlhelp_basename = "rmy-yeah doc"

intersphinx_mapping = {"python": ("https://docs.python.org/3/", None)}

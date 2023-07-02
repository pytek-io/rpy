from __future__ import annotations

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


v = parse(get_version("rmy"))
version = v.base_version
release = v.public

language = "en"

exclude_patterns = ["_build"]
pygments_style = "sphinx"
autodoc_default_options = {"members": True, "show-inheritance": True}
todo_include_todos = False

html_title = "RMY"
html_theme = "furo"
htmlhelp_basename = "rmy-yeah doc"

intersphinx_mapping = {"python": ("https://docs.python.org/3/", None)}

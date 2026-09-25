# Python client reference, built by sphinx-autoapi from the type stub.
#
# Everything is read from datahub_python_bindings/python/intellistream_datahub_sdk/__init__.pyi,
# statically, so the build needs neither cargo nor the compiled module. The stub is the
# single source for Python-facing signatures and prose.

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "_ext"))

project = "IntelliStream DataHub — Python client"
copyright = "IntelliStream"
author = "IntelliStream"

extensions = ["autoapi.extension", "sphinx.ext.napoleon", "sphinx.ext.intersphinx", "service_pages"]

# The service classes are skipped by autoapi and given pages of their own, named the way a
# caller reaches them (`timeseries.by_ids`); see _ext/service_pages.py and structure.toml.
service_pages_stub = "../datahub_python_bindings/python/intellistream_datahub_sdk/__init__.pyi"

autoapi_type = "python"
autoapi_dirs = ["../datahub_python_bindings/python"]
autoapi_file_patterns = ["*.pyi", "*.py"]
autoapi_root = "api"
autoapi_template_dir = "_templates/autoapi"
autoapi_add_toctree_entry = False
autoapi_member_order = "groupwise"
autoapi_own_page_level = "class"
autoapi_options = ["members", "undoc-members", "show-module-summary", "imported-members"]

html_theme = "sphinx_rtd_theme"
html_title = project
html_show_sourcelink = False
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_theme_options = {"navigation_depth": 2, "collapse_navigation": False}

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# Name things the way a caller writes them: `TimeSeries`, not the fully qualified path.
add_module_names = False
toc_object_entries_show_parents = "hide"
python_use_unqualified_type_names = True

exclude_patterns = ["_build", "_templates"]

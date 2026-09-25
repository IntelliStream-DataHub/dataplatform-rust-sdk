"""Sphinx extension: one page per client service, laid out the way a caller reaches it.

autoapi documents classes, and a service class is not something a caller ever names: it is
handed back by `client.timeseries`, never imported. So autoapi is told to skip the service
classes, and this writes their pages instead -- `timeseries.by_ids(...)` rather than
`TimeSeriesServiceSync.by_ids(...)`, methods grouped by structure.toml rather than sorted,
and the sync and async clients side by side rather than on two pages.

Everything on those pages comes from the type stub: signatures, defaults and docstrings.
structure.toml says only which heading a method sits under. Drift between the two is a
Sphinx warning, so a build with -W fails until the structure file catches up.
"""

from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path

from sphinx.application import Sphinx
from sphinx.ext.napoleon import Config as NapoleonConfig
from sphinx.ext.napoleon.docstring import NumpyDocstring
from sphinx.util import logging

logger = logging.getLogger(__name__)

MODULE = "intellistream_datahub_sdk"
SERVICE = re.compile(r"^(\w+)Service(Sync|Async)$")
UNLISTED = "Other"
OUT = "services"


def unparse_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """The signature as a reader writes it.

    `builtins.` is dropped: the stub needs it inside services, where `def list` shadows the
    builtin for the rest of the class body, but on the page `list[TimeSeries]` is unambiguous.
    """
    args = re.sub(r"^self(,\s*)?", "", ast.unparse(node.args))
    returns = f" -> {ast.unparse(node.returns)}" if node.returns else ""
    return re.sub(r"\bbuiltins\.", "", f"{node.name}({args}){returns}")


def returns(signature: str) -> str:
    return signature.rsplit(" -> ", 1)[1] if " -> " in signature else ""


def parameter_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    a = node.args
    names = [p.arg for p in a.posonlyargs + a.args + a.kwonlyargs]
    return [n for n in names if n not in ("self", "cls")]


def read_services(stub: Path) -> tuple[dict[str, dict], dict[str, str]]:
    """`base -> {"sync"|"async": {"doc", "methods": {name: node}}}`, and `base -> accessor`."""
    tree = ast.parse(stub.read_text(encoding="utf8"))
    services: dict[str, dict] = {}
    accessor: dict[str, str] = {}

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        m = SERVICE.match(node.name)
        if m:
            methods = {
                item.name: item
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and not item.name.startswith("_")
            }
            services.setdefault(m.group(1), {})[m.group(2).lower()] = {
                "doc": ast.get_docstring(node),
                "methods": methods,
            }
        elif node.name == "DataHubClient":
            # The client's getters are what say `client.datasets` reaches DatasetsServiceSync.
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.returns is not None:
                    r = SERVICE.match(ast.unparse(item.returns))
                    if r:
                        accessor[r.group(1)] = item.name

    return services, accessor


def indent(text: str, by: str = "   ") -> str:
    return "\n".join(by + line if line.strip() else "" for line in text.split("\n"))


def docstring(text: str | None, napoleon: NapoleonConfig) -> str:
    """The stub's docstring as reST, numpy-style sections included, as autoapi renders them."""
    return str(NumpyDocstring(text, napoleon)).strip() if text else ""


def call_cell(node, path: str, prefix: str = "") -> str:
    if node is None:
        return "—"
    names = parameter_names(node)
    shown = ", ".join(names) if len(names) <= 3 else "…"
    cell = f"{prefix}:py:meth:`{node.name}({shown}) <{path}.{node.name}>`"
    result = returns(unparse_signature(node))
    return cell + (f" → ``{result}``" if result else "")


def service_page(base: str, variants: dict, accessor: str, groups: list[dict],
                 text: dict[str, str], napoleon: NapoleonConfig) -> tuple[str, set[str]]:
    sync = variants.get("sync", {}).get("methods", {})
    aio = variants.get("async", {}).get("methods", {})
    names = list(sync) + [n for n in aio if n not in sync]

    def group_of(name: str) -> str:
        return next((g["title"] for g in groups if name in g.get("methods", ())), UNLISTED)

    by_group: dict[str, list[str]] = {}
    for name in names:
        by_group.setdefault(group_of(name), []).append(name)
    ordered = [(g["title"], g.get("blurb", "")) for g in groups] + [(UNLISTED, text.get("unlisted_blurb", ""))]
    in_order = [n for title, _ in ordered for n in by_group.get(title, [])]

    title = f"``{accessor}``"
    out = [title, "=" * len(title), "", f".. py:currentmodule:: {MODULE}", ""]
    doc = variants.get("sync", {}).get("doc") or variants.get("async", {}).get("doc")
    if doc:
        out += [docstring(doc, napoleon), ""]
    out += [".. code-block:: python", "",
            f"   from {MODULE} import DataHubClient", "",
            "   client = DataHubClient.from_env()",
            f"   result = client.{accessor}.{in_order[0]}(...)", ""]

    out += [".. list-table::", "   :header-rows: 1", "   :widths: 1 1", "",
            "   * - ``DataHubClient``", "     - ``AsyncDataHubClient``"]
    for name in in_order:
        out += [f"   * - {call_cell(sync.get(name), accessor)}",
                f"     - {call_cell(aio.get(name), accessor, 'await ')}"]
    out.append("")

    for title, blurb in ordered:
        members = by_group.get(title)
        if not members:
            continue
        if len(by_group) > 1:
            out += [title, "-" * len(title), ""]
            if blurb:
                out += [blurb, ""]
        for name in members:
            s, a = sync.get(name), aio.get(name)
            node = s or a
            out += [f".. py:method:: {accessor}.{unparse_signature(node)}", ""]
            body = docstring(ast.get_docstring(node) or (a and ast.get_docstring(a)), napoleon)
            if body:
                out += [indent(body), ""]
            note = ""
            if s is None:
                note = text.get("async_only", "")
            elif a is None:
                note = text.get("sync_only", "")
            elif returns(unparse_signature(s)) != returns(unparse_signature(a)):
                note = text.get("async_returns", "").format(returns=returns(unparse_signature(a)))
            if note:
                out += ["   .. note::", "", f"      {note}", ""]

    return "\n".join(out) + "\n", set(names)


def generate(app: Sphinx) -> None:
    src = Path(app.srcdir)
    stub = (src / app.config.service_pages_stub).resolve()
    structure = tomllib.loads((src / "structure.toml").read_text(encoding="utf8"))
    order, groups, text = structure.get("order", []), structure.get("groups", []), structure.get("text", {})
    napoleon = NapoleonConfig(napoleon_use_param=True, napoleon_use_rtype=True)

    services, accessor = read_services(stub)
    ranked = sorted(services, key=lambda b: (order.index(b) if b in order else len(order), b))

    out = src / OUT
    out.mkdir(exist_ok=True)
    for stale in out.glob("*.rst"):
        stale.unlink()

    present: set[str] = set()
    pages = []
    for base in ranked:
        if base not in order:
            logger.warning("service %s is not in structure.toml's order", base)
        if base not in accessor:
            logger.warning("no DataHubClient getter returns %sServiceSync", base)
        page, names = service_page(base, services[base], accessor.get(base, base.lower()), groups, text, napoleon)
        present |= names
        slug = accessor.get(base, base.lower())
        (out / f"{slug}.rst").write_text(page, encoding="utf8")
        pages.append(slug)

    listed = {m for g in groups for m in g.get("methods", ())}
    for name in sorted(present - listed):
        logger.warning("method %s() is in no structure.toml group; it renders under '%s'", name, UNLISTED)
    for name in sorted(listed - present):
        logger.warning("structure.toml lists %s(), which no service has", name)

    # The landing page includes this list, so its services are the ones generated here.
    listing = [".. rst-class:: service-list", ""] + [f"- :doc:`{OUT}/{slug}`" for slug in pages]
    (out / "list.inc").write_text("\n".join(listing) + "\n", encoding="utf8")

    title = text.get("services_title", "Services")
    index = [title, "=" * len(title), "", text.get("services_blurb", ""), "",
             ".. toctree::", "   :maxdepth: 1", ""] + [f"   {p}" for p in pages]
    (out / "index.rst").write_text("\n".join(index) + "\n", encoding="utf8")


def skip_service_classes(app, what, name, obj, skip, options):
    """autoapi-skip-member: the service classes get this extension's pages instead."""
    if what == "class" and SERVICE.match(name.rsplit(".", 1)[-1]):
        return True
    return None


def setup(app: Sphinx) -> dict:
    app.add_config_value("service_pages_stub", "", "env")
    app.connect("builder-inited", generate)
    app.connect("autoapi-skip-member", skip_service_classes)
    return {"parallel_read_safe": True, "parallel_write_safe": True}

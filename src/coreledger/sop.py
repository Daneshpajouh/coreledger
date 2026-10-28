"""Draft standard operating procedures from a pipeline parameter schema and a service params file.

A sequencing or analysis core runs a public pipeline (for example nf-core/rnaseq) with its own
pinned parameter file. This module turns the pipeline's JSON Schema plus the core's params file
into a draft SOP in Markdown, checks the params against the schema, and, given the schema of the
previous pipeline release, lists what changed between releases.

Schema support: nf-core style schemas with top-level ``$defs`` (2020-12) or ``definitions``
(draft-07) groups, each holding ``properties`` and ``required``, ordered by the top-level
``allOf`` ``$ref`` list, then any groups not referenced from ``allOf``, then any top-level
``properties``/``required`` (this also covers a plain JSON Schema with only top-level
properties, such as a Snakemake config schema). Only top-level object properties are read;
properties nested inside a nested object schema are not. Schema files must be JSON; YAML is
not supported.
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


class _Missing:
    """Sentinel for 'the schema gives no default', distinct from a default of None."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "MISSING"


MISSING = _Missing()


@dataclass(frozen=True)
class Param:
    """One parameter as described by a pipeline's parameter schema."""

    name: str
    group: str
    type: Optional[str]
    default: Any
    description: str
    enum: Optional[Tuple[Any, ...]]
    pattern: Optional[str]
    minimum: Optional[Union[int, float]]
    maximum: Optional[Union[int, float]]
    required: bool
    hidden: bool


@dataclass(frozen=True)
class Issue:
    """One problem found while checking a params file against a schema."""

    level: str
    param: str
    message: str


@dataclass(frozen=True)
class SchemaDiff:
    """What changed between two versions of a pipeline's parameter schema."""

    added: Tuple[str, ...]
    removed: Tuple[str, ...]
    default_changed: Tuple[Tuple[str, Any, Any], ...]
    type_changed: Tuple[Tuple[str, Optional[str], Optional[str]], ...]
    enum_changed: Tuple[Tuple[str, Optional[Tuple[Any, ...]], Optional[Tuple[Any, ...]]], ...]


def _group_name_from_ref(ref: str) -> str:
    return ref.rsplit("/", 1)[-1]


def _make_param(name: str, schema: Dict[str, Any], group: str, required: bool) -> Param:
    type_field = schema.get("type")
    if isinstance(type_field, list):
        type_str: Optional[str] = "|".join(str(t) for t in type_field)
    elif type_field is None:
        type_str = None
    else:
        type_str = str(type_field)

    default = schema["default"] if "default" in schema else MISSING

    enum = schema.get("enum")
    enum_tuple = tuple(enum) if enum is not None else None

    return Param(
        name=name,
        group=group,
        type=type_str,
        default=default,
        description=schema.get("description") or "",
        enum=enum_tuple,
        pattern=schema.get("pattern"),
        minimum=schema.get("minimum"),
        maximum=schema.get("maximum"),
        required=required,
        hidden=bool(schema.get("hidden", False)),
    )


def load_schema(path: Union[str, Path]) -> Dict[str, Param]:
    """Load a pipeline parameter schema into an insertion-ordered mapping of name to `Param`.

    Supports nf-core style schemas (top-level ``$defs`` or ``definitions`` groups ordered by
    ``allOf``) and plain JSON Schemas with only top-level ``properties``. Only JSON is
    supported; a file that does not parse as JSON raises `ValueError`. A parameter name that
    appears in more than one place in the schema raises `ValueError`.
    """
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{path} is not valid JSON; load_schema supports JSON schemas only "
            "(a YAML Snakemake schema must be converted to JSON first)"
        ) from exc
    if not isinstance(doc, dict):
        raise ValueError(f"{path} does not contain a JSON object at the top level")

    if "$defs" in doc:
        groups = doc["$defs"]
    elif "definitions" in doc:
        groups = doc["definitions"]
    else:
        groups = {}

    ordered_group_names: List[str] = []
    for entry in doc.get("allOf", []) or []:
        ref = entry.get("$ref") if isinstance(entry, dict) else None
        if not ref:
            continue
        name = _group_name_from_ref(ref)
        if name in groups and name not in ordered_group_names:
            ordered_group_names.append(name)
    for name in groups:
        if name not in ordered_group_names:
            ordered_group_names.append(name)

    params: Dict[str, Param] = {}

    def add_properties(properties: Dict[str, Any], required_names: Any, group_title: str) -> None:
        required_set = set(required_names or [])
        for pname, pschema in properties.items():
            if pname in params:
                raise ValueError(f"duplicate parameter name across schema groups: {pname}")
            params[pname] = _make_param(pname, pschema, group_title, pname in required_set)

    for gname in ordered_group_names:
        group = groups[gname]
        add_properties(group.get("properties", {}) or {}, group.get("required"), group.get("title") or "")

    add_properties(doc.get("properties", {}) or {}, doc.get("required"), "")

    return params


def load_params(path: Union[str, Path]) -> Dict[str, Any]:
    """Load a service's pinned params file (JSON only)."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _value_matches_type(value: Any, type_name: str) -> bool:
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "null":
        return value is None
    if type_name == "array":
        return isinstance(value, list)
    if type_name == "object":
        return isinstance(value, dict)
    return True


def _check_value(name: str, param: Param, value: Any) -> List[Issue]:
    issues: List[Issue] = []

    if param.type is not None:
        allowed = param.type.split("|")
        if not any(_value_matches_type(value, t) for t in allowed):
            issues.append(Issue("error", name, f"value does not match expected type '{param.type}'"))
            return issues

    if param.enum is not None and value not in param.enum:
        issues.append(Issue("error", name, f"value {value!r} is not one of the allowed values {list(param.enum)!r}"))

    if param.pattern is not None and isinstance(value, str) and re.search(param.pattern, value) is None:
        issues.append(Issue("error", name, f"value {value!r} does not match pattern '{param.pattern}'"))

    is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
    if param.minimum is not None and is_number and value < param.minimum:
        issues.append(Issue("error", name, f"value {value!r} is below the minimum {param.minimum!r}"))
    if param.maximum is not None and is_number and value > param.maximum:
        issues.append(Issue("error", name, f"value {value!r} is above the maximum {param.maximum!r}"))

    if param.default is not MISSING and type(value) is type(param.default) and value == param.default:
        issues.append(Issue("warning", name, "value is set to the schema default"))

    return issues


def check_params(schema: Dict[str, Param], params: Dict[str, Any]) -> List[Issue]:
    """Check a service's params against a pipeline schema and return issues in schema order.

    Unknown parameters (present in ``params`` but not in ``schema``) are reported last, sorted
    by name.
    """
    issues: List[Issue] = []

    for name, param in schema.items():
        if name in params:
            issues.extend(_check_value(name, param, params[name]))
        elif param.required and param.default is MISSING:
            issues.append(Issue("error", name, "required parameter is missing and has no default"))

    for name in sorted(set(params) - set(schema)):
        issues.append(Issue("error", name, "parameter is not defined in the schema"))

    return issues


def diff_schemas(old: Dict[str, Param], new: Dict[str, Param]) -> SchemaDiff:
    """Compare two versions of a pipeline schema. Names in the result are sorted."""
    old_names = set(old)
    new_names = set(new)
    added = tuple(sorted(new_names - old_names))
    removed = tuple(sorted(old_names - new_names))

    default_changed = []
    type_changed = []
    enum_changed = []
    for name in sorted(old_names & new_names):
        old_param = old[name]
        new_param = new[name]
        if old_param.default is MISSING or new_param.default is MISSING:
            if old_param.default is not new_param.default:
                default_changed.append((name, old_param.default, new_param.default))
        elif old_param.default != new_param.default:
            default_changed.append((name, old_param.default, new_param.default))
        if old_param.type != new_param.type:
            type_changed.append((name, old_param.type, new_param.type))
        if old_param.enum != new_param.enum:
            enum_changed.append((name, old_param.enum, new_param.enum))

    return SchemaDiff(
        added=added,
        removed=removed,
        default_changed=tuple(default_changed),
        type_changed=tuple(type_changed),
        enum_changed=tuple(enum_changed),
    )


def _cell(value: Any) -> str:
    text = str(value)
    text = text.replace("\n", " ")
    text = text.replace("|", "\\|")
    return text


def _first_sentence(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"[.!?](\s|$)", text)
    if match:
        return text[: match.start() + 1]
    return text


def _change_note(pinned: bool, old_default: Any, new_default: Any) -> str:
    # A schema that starts or stops stating a default says nothing about the value a run
    # gets: that comes from the pipeline's nextflow.config, which this module does not read.
    if pinned:
        return "(pinned)"
    if old_default is MISSING:
        return "(not stated in the old schema; compare the old release's nextflow.config)"
    if new_default is MISSING:
        return "(no longer stated in the schema; check the new release's nextflow.config)"
    return "(not pinned by this service: the default changes on upgrade)"


def _json_display(value: Any) -> str:
    if value is MISSING:
        return "none"
    return json.dumps(value)


def _table(headers: Tuple[str, ...], rows: List[Tuple[Any, ...]]) -> List[str]:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(_cell(cell) for cell in row) + " |")
    return lines


def render_sop(
    schema: Dict[str, Param],
    params: Dict[str, Any],
    pipeline: str,
    version: str,
    profile: Optional[str] = None,
    old_schema: Optional[Dict[str, Param]] = None,
    old_version: Optional[str] = None,
) -> str:
    """Render a draft Markdown SOP for a pipeline run, given its schema and a service params file.

    When ``old_schema`` (and ``old_version``) are given, an extra section lists what changed
    in the schema since that release, and marks default changes for parameters the service
    does not pin, since those reach its runs without any edit to its params file.
    """
    lines: List[str] = []

    lines.append(f"# {pipeline} {version}: standard operating procedure (draft)")
    lines.append("")
    lines.append(
        "This draft was generated from the pipeline's parameter schema and this service's "
        "params file. Review it before use."
    )
    lines.append("")

    lines.append("## Run command")
    lines.append("")
    run_command = f"nextflow run {pipeline} -r {version} -params-file params.json"
    if profile:
        run_command += f" -profile {profile}"
    lines.append("```")
    lines.append(run_command)
    lines.append("```")
    lines.append("")

    lines.append("## Parameters this service sets")
    lines.append("")
    set_rows = []
    for name, param in schema.items():
        if name in params:
            set_rows.append(
                (
                    name,
                    json.dumps(params[name]),
                    _json_display(param.default),
                    _first_sentence(param.description),
                )
            )
    if set_rows:
        lines.extend(_table(("Parameter", "Value", "Pipeline default", "What it does"), set_rows))
    else:
        lines.append("None.")
    lines.append("")

    lines.append("## Pipeline defaults this service relies on")
    lines.append("")
    default_rows = []
    for name, param in schema.items():
        if not param.hidden and param.default is not MISSING and name not in params:
            default_rows.append((name, json.dumps(param.default), _first_sentence(param.description)))
    if default_rows:
        lines.extend(_table(("Parameter", "Default", "What it does"), default_rows))
    else:
        lines.append("None.")
    lines.append("")

    lines.append("## Required inputs")
    lines.append("")
    required_names = [name for name, param in schema.items() if param.required]
    if required_names:
        for name in required_names:
            description = _first_sentence(schema[name].description)
            if description:
                lines.append(f"- `{name}`: {description}")
            else:
                lines.append(f"- `{name}`")
    else:
        lines.append("None.")
    lines.append("")

    lines.append("## Checks")
    lines.append("")
    issues = check_params(schema, params)
    if issues:
        for issue in issues:
            lines.append(f"- {issue.level}: `{issue.param}`: {issue.message}")
    else:
        lines.append("No problems found.")
    lines.append("")

    if old_schema is not None:
        lines.append(f"## Changes since {old_version}")
        lines.append("")
        diff = diff_schemas(old_schema, schema)

        lines.append("### Added")
        lines.append("")
        if diff.added:
            for name in diff.added:
                lines.append(f"- `{name}`")
        else:
            lines.append("None.")
        lines.append("")

        lines.append("### Removed")
        lines.append("")
        if diff.removed:
            for name in diff.removed:
                lines.append(f"- `{name}`")
        else:
            lines.append("None.")
        lines.append("")

        lines.append("### Default changes")
        lines.append("")
        if diff.default_changed:
            for name, old_default, new_default in diff.default_changed:
                lines.append(
                    f"- `{name}`: {_json_display(old_default)} to {_json_display(new_default)} "
                    f"{_change_note(name in params, old_default, new_default)}"
                )
        else:
            lines.append("None.")
        lines.append("")

        lines.append("### Type changes")
        lines.append("")
        if diff.type_changed:
            for name, old_type, new_type in diff.type_changed:
                lines.append(f"- `{name}`: {old_type or 'none'} to {new_type or 'none'}")
        else:
            lines.append("None.")
        lines.append("")

        lines.append("### Enum changes")
        lines.append("")
        if diff.enum_changed:
            for name, old_enum, new_enum in diff.enum_changed:
                old_disp = list(old_enum) if old_enum is not None else "none"
                new_disp = list(new_enum) if new_enum is not None else "none"
                lines.append(f"- `{name}`: {old_disp} to {new_disp}")
        else:
            lines.append("None.")
        lines.append("")

    return "\n".join(lines).rstrip("\n") + "\n"

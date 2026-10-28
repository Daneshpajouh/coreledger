"""Tests for coreledger.sop."""

import dataclasses
import json
from pathlib import Path

import pytest

from coreledger.sop import (
    MISSING,
    Issue,
    Param,
    SchemaDiff,
    check_params,
    diff_schemas,
    load_params,
    load_schema,
    render_sop,
)

FIXTURES = Path(__file__).parent / "fixtures"
OLD_SCHEMA_PATH = FIXTURES / "rnaseq_3.14.0_nextflow_schema.json"
NEW_SCHEMA_PATH = FIXTURES / "rnaseq_3.27.0_nextflow_schema.json"
SERVICE_PARAMS_PATH = FIXTURES / "rnaseq_service_params.json"


def write_schema(tmp_path: Path, name: str, doc: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# load_schema: real nf-core fixtures
# ---------------------------------------------------------------------------


def test_load_schema_counts_match_known_answers():
    old_schema = load_schema(OLD_SCHEMA_PATH)
    new_schema = load_schema(NEW_SCHEMA_PATH)
    assert len(old_schema) == 109
    assert len(new_schema) == 136


def test_load_schema_is_insertion_ordered_by_allof():
    schema = load_schema(NEW_SCHEMA_PATH)
    names = list(schema)
    # input_output_options is the first group in allOf, and "input" is its first property.
    assert names[0] == "input"
    assert names[1] == "outdir"


def test_diff_schemas_matches_known_answers():
    old_schema = load_schema(OLD_SCHEMA_PATH)
    new_schema = load_schema(NEW_SCHEMA_PATH)
    diff = diff_schemas(old_schema, new_schema)
    assert len(diff.added) == 35
    assert len(diff.removed) == 8
    changed_names = tuple(name for name, _, _ in diff.default_changed)
    assert changed_names == (
        "igenomes_base",
        "skip_biotype_qc",
        "skip_deseq2_qc",
        "skip_dupradar",
        "skip_qualimap",
        "skip_rseqc",
    )


# ---------------------------------------------------------------------------
# load_schema: shapes
# ---------------------------------------------------------------------------


def test_load_schema_draft07_definitions(tmp_path):
    doc = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "definitions": {
            "group_one": {
                "title": "Group one",
                "properties": {"alpha": {"type": "string", "description": "An alpha param."}},
                "required": ["alpha"],
            }
        },
        "allOf": [{"$ref": "#/definitions/group_one"}],
    }
    path = write_schema(tmp_path, "draft07.json", doc)
    schema = load_schema(path)
    assert list(schema) == ["alpha"]
    assert schema["alpha"].group == "Group one"
    assert schema["alpha"].required is True


def test_load_schema_plain_top_level_properties(tmp_path):
    doc = {
        "type": "object",
        "properties": {
            "threads": {"type": "integer", "default": 4, "description": "Number of threads."},
            "sample": {"type": "string"},
        },
        "required": ["sample"],
    }
    path = write_schema(tmp_path, "plain.json", doc)
    schema = load_schema(path)
    assert list(schema) == ["threads", "sample"]
    assert schema["threads"].default == 4
    assert schema["sample"].required is True
    assert schema["threads"].group == ""


def test_load_schema_unreferenced_group_comes_after_referenced(tmp_path):
    doc = {
        "$defs": {
            "unreferenced": {"properties": {"z_param": {"type": "string"}}},
            "referenced": {"properties": {"a_param": {"type": "string"}}},
        },
        "allOf": [{"$ref": "#/$defs/referenced"}],
    }
    path = write_schema(tmp_path, "unref.json", doc)
    schema = load_schema(path)
    assert list(schema) == ["a_param", "z_param"]


def test_load_schema_groups_then_top_level_properties(tmp_path):
    doc = {
        "$defs": {"group_one": {"properties": {"a_param": {"type": "string"}}}},
        "allOf": [{"$ref": "#/$defs/group_one"}],
        "properties": {"top_param": {"type": "string"}},
    }
    path = write_schema(tmp_path, "mixed.json", doc)
    schema = load_schema(path)
    assert list(schema) == ["a_param", "top_param"]


def test_load_schema_duplicate_name_raises(tmp_path):
    doc = {
        "$defs": {
            "group_one": {"properties": {"dupe": {"type": "string"}}},
            "group_two": {"properties": {"dupe": {"type": "integer"}}},
        },
        "allOf": [{"$ref": "#/$defs/group_one"}, {"$ref": "#/$defs/group_two"}],
    }
    path = write_schema(tmp_path, "dupe.json", doc)
    with pytest.raises(ValueError, match="duplicate"):
        load_schema(path)


def test_load_schema_non_json_file_raises_clear_error(tmp_path):
    path = tmp_path / "schema.yaml"
    path.write_text("properties:\n  sample:\n    type: string\n", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON"):
        load_schema(path)


def test_load_schema_nested_object_properties_not_expanded(tmp_path):
    doc = {
        "properties": {
            "settings": {
                "type": "object",
                "properties": {"inner": {"type": "string"}},
            }
        }
    }
    path = write_schema(tmp_path, "nested.json", doc)
    schema = load_schema(path)
    assert list(schema) == ["settings"]
    assert schema["settings"].type == "object"


def test_load_schema_missing_vs_null_default(tmp_path):
    doc = {
        "properties": {
            "no_default": {"type": "string"},
            "null_default": {"type": "string", "default": None},
        }
    }
    path = write_schema(tmp_path, "defaults.json", doc)
    schema = load_schema(path)
    assert schema["no_default"].default is MISSING
    assert schema["null_default"].default is None


def test_load_schema_multiple_types_joined_with_pipe(tmp_path):
    doc = {"properties": {"help": {"type": ["boolean", "string"]}}}
    path = write_schema(tmp_path, "multi.json", doc)
    schema = load_schema(path)
    assert schema["help"].type == "boolean|string"


def test_load_params_reads_json(tmp_path):
    path = tmp_path / "params.json"
    path.write_text(json.dumps({"input": "a.csv"}), encoding="utf-8")
    assert load_params(path) == {"input": "a.csv"}


def test_load_params_real_fixture_is_dict():
    params = load_params(SERVICE_PARAMS_PATH)
    assert params["input"] == "samplesheet.csv"


# ---------------------------------------------------------------------------
# check_params
# ---------------------------------------------------------------------------


def make_schema(**params: Param):
    return dict(params)


def param(**kwargs):
    base = dict(
        name="p",
        group="",
        type=None,
        default=MISSING,
        description="",
        enum=None,
        pattern=None,
        minimum=None,
        maximum=None,
        required=False,
        hidden=False,
    )
    base.update(kwargs)
    return Param(**base)


def test_check_params_unknown_param_is_error():
    schema = make_schema(known=param(name="known", type="string"))
    issues = check_params(schema, {"known": "x", "unknown": 1})
    assert Issue("error", "unknown", "parameter is not defined in the schema") in issues


def test_check_params_known_param_is_not_flagged_as_unknown():
    schema = make_schema(known=param(name="known", type="string"))
    issues = check_params(schema, {"known": "x"})
    assert all(issue.param != "known" or issue.level != "error" for issue in issues)


def test_check_params_string_type_pass_and_fail():
    schema = make_schema(s=param(name="s", type="string"))
    assert check_params(schema, {"s": "ok"}) == []
    issues = check_params(schema, {"s": 1})
    assert issues[0].level == "error"
    assert issues[0].param == "s"


def test_check_params_bool_is_not_integer():
    schema = make_schema(n=param(name="n", type="integer"))
    assert check_params(schema, {"n": 2}) == []
    issues = check_params(schema, {"n": True})
    assert len(issues) == 1 and issues[0].level == "error"


def test_check_params_integral_float_is_not_integer():
    schema = make_schema(n=param(name="n", type="integer"))
    issues = check_params(schema, {"n": 2.0})
    assert len(issues) == 1 and issues[0].level == "error"


def test_check_params_number_allows_int_and_float_not_bool():
    schema = make_schema(n=param(name="n", type="number"))
    assert check_params(schema, {"n": 2}) == []
    assert check_params(schema, {"n": 2.5}) == []
    issues = check_params(schema, {"n": True})
    assert len(issues) == 1 and issues[0].level == "error"


def test_check_params_boolean_pass_and_fail():
    schema = make_schema(b=param(name="b", type="boolean"))
    assert check_params(schema, {"b": True}) == []
    issues = check_params(schema, {"b": "yes"})
    assert len(issues) == 1 and issues[0].level == "error"


def test_check_params_null_pass_and_fail():
    schema = make_schema(n=param(name="n", type="null"))
    assert check_params(schema, {"n": None}) == []
    issues = check_params(schema, {"n": "x"})
    assert len(issues) == 1 and issues[0].level == "error"


def test_check_params_multiple_allowed_types():
    schema = make_schema(h=param(name="h", type="boolean|string"))
    assert check_params(schema, {"h": True}) == []
    assert check_params(schema, {"h": "message.txt"}) == []
    issues = check_params(schema, {"h": 5})
    assert len(issues) == 1 and issues[0].level == "error"


def test_check_params_enum_pass_and_fail():
    schema = make_schema(e=param(name="e", type="string", enum=("a", "b")))
    assert check_params(schema, {"e": "a"}) == []
    issues = check_params(schema, {"e": "c"})
    assert len(issues) == 1 and issues[0].level == "error"


def test_check_params_pattern_uses_search_not_fullmatch():
    schema = make_schema(p=param(name="p", type="string", pattern=r"\d{3}"))
    # re.search: the digits do not need to span the whole string.
    assert check_params(schema, {"p": "abc123def"}) == []
    issues = check_params(schema, {"p": "no digits here"})
    assert len(issues) == 1 and issues[0].level == "error"


def test_check_params_minimum_maximum_pass_and_fail():
    schema = make_schema(n=param(name="n", type="number", minimum=0, maximum=10))
    assert check_params(schema, {"n": 5}) == []
    low = check_params(schema, {"n": -1})
    high = check_params(schema, {"n": 11})
    assert len(low) == 1 and low[0].level == "error"
    assert len(high) == 1 and high[0].level == "error"


def test_check_params_required_missing_no_default_is_error():
    schema = make_schema(r=param(name="r", required=True))
    issues = check_params(schema, {})
    assert len(issues) == 1
    assert issues[0].level == "error"
    assert issues[0].param == "r"


def test_check_params_required_missing_with_default_is_not_error():
    schema = make_schema(r=param(name="r", required=True, default="fallback"))
    assert check_params(schema, {}) == []


def test_check_params_warning_when_set_to_default():
    schema = make_schema(d=param(name="d", type="string", default="trimgalore"))
    issues = check_params(schema, {"d": "trimgalore"})
    assert issues == [Issue("warning", "d", "value is set to the schema default")]


def test_check_params_warning_uses_equality_not_identity():
    # Build a value that equals the default but is a different object, so a buggy "is"
    # comparison (instead of "==") would wrongly miss the warning.
    schema = make_schema(n=param(name="n", type="integer", default=12345))
    value = int(str(12345))
    assert value == 12345 and value is not schema["n"].default
    issues = check_params(schema, {"n": value})
    assert issues == [Issue("warning", "n", "value is set to the schema default")]


def test_check_params_no_warning_bool_default_vs_int_value():
    # True == 1 in Python; the type() guard must stop this being flagged as "equal to default".
    schema = make_schema(d=param(name="d", type="integer", default=True))
    assert check_params(schema, {"d": 1}) == []


def test_check_params_deterministic_order_unknown_last_sorted():
    schema = make_schema(
        first=param(name="first", type="string"),
        second=param(name="second", type="string"),
    )
    issues = check_params(schema, {"first": 1, "second": 2, "zeta": 1, "alpha": 1})
    names_in_order = [issue.param for issue in issues]
    assert names_in_order == ["first", "second", "alpha", "zeta"]


# ---------------------------------------------------------------------------
# diff_schemas
# ---------------------------------------------------------------------------


def test_diff_schemas_added_removed():
    old = make_schema(kept=param(name="kept"), gone=param(name="gone"))
    new = make_schema(kept=param(name="kept"), fresh=param(name="fresh"))
    diff = diff_schemas(old, new)
    assert diff.added == ("fresh",)
    assert diff.removed == ("gone",)


def test_diff_schemas_default_changed_value_to_value():
    old = make_schema(p=param(name="p", default="a"))
    new = make_schema(p=param(name="p", default="b"))
    diff = diff_schemas(old, new)
    assert diff.default_changed == (("p", "a", "b"),)


def test_diff_schemas_default_changed_missing_to_value_and_back():
    old = make_schema(p=param(name="p", default=MISSING))
    new = make_schema(p=param(name="p", default=False))
    diff = diff_schemas(old, new)
    assert diff.default_changed == (("p", MISSING, False),)
    diff_back = diff_schemas(new, old)
    assert diff_back.default_changed == (("p", False, MISSING),)


def test_diff_schemas_no_change_when_both_missing():
    old = make_schema(p=param(name="p", default=MISSING))
    new = make_schema(p=param(name="p", default=MISSING))
    diff = diff_schemas(old, new)
    assert diff.default_changed == ()


def test_diff_schemas_type_changed():
    old = make_schema(p=param(name="p", type="string"))
    new = make_schema(p=param(name="p", type="integer"))
    diff = diff_schemas(old, new)
    assert diff.type_changed == (("p", "string", "integer"),)


def test_diff_schemas_enum_changed():
    old = make_schema(p=param(name="p", enum=("a", "b")))
    new = make_schema(p=param(name="p", enum=("a", "b", "c")))
    diff = diff_schemas(old, new)
    assert diff.enum_changed == (("p", ("a", "b"), ("a", "b", "c")),)


def test_diff_schemas_names_sorted():
    old = make_schema(zeta=param(name="zeta"), alpha=param(name="alpha"))
    new = make_schema(zeta=param(name="zeta", default=1), alpha=param(name="alpha", default=1))
    diff = diff_schemas(old, new)
    names = tuple(name for name, _, _ in diff.default_changed)
    assert names == ("alpha", "zeta")


def test_schema_diff_is_a_plain_frozen_dataclass():
    diff = SchemaDiff(added=(), removed=(), default_changed=(), type_changed=(), enum_changed=())
    with pytest.raises(dataclasses.FrozenInstanceError):
        diff.added = ("x",)


# ---------------------------------------------------------------------------
# render_sop
# ---------------------------------------------------------------------------

SECTION_HEADINGS = (
    "## Run command",
    "## Parameters this service sets",
    "## Pipeline defaults this service relies on",
    "## Required inputs",
    "## Checks",
)


def test_render_sop_contains_all_sections_on_real_fixtures():
    old_schema = load_schema(OLD_SCHEMA_PATH)
    new_schema = load_schema(NEW_SCHEMA_PATH)
    params = load_params(SERVICE_PARAMS_PATH)
    text = render_sop(
        new_schema,
        params,
        pipeline="nf-core/rnaseq",
        version="3.27.0",
        profile="singularity",
        old_schema=old_schema,
        old_version="3.14.0",
    )
    assert text.startswith("# nf-core/rnaseq 3.27.0: standard operating procedure (draft)")
    for heading in SECTION_HEADINGS:
        assert heading in text
    assert "## Changes since 3.14.0" in text
    assert "### Added" in text
    assert "### Removed" in text
    assert "### Default changes" in text
    assert "### Type changes" in text
    assert "### Enum changes" in text


def test_render_sop_marks_unpinned_default_change():
    old_schema = load_schema(OLD_SCHEMA_PATH)
    new_schema = load_schema(NEW_SCHEMA_PATH)
    params = load_params(SERVICE_PARAMS_PATH)
    assert "skip_qualimap" not in params
    text = render_sop(
        new_schema,
        params,
        pipeline="nf-core/rnaseq",
        version="3.27.0",
        old_schema=old_schema,
        old_version="3.14.0",
    )
    lines = [line for line in text.splitlines() if "`skip_qualimap`" in line]
    assert len(lines) == 1
    # 3.14.0's schema gives no default for skip_qualimap; 3.27.0's says false
    assert lines[0] == (
        "- `skip_qualimap`: none to false (not stated in the old schema; compare the old release's nextflow.config)"
    )
    lines = [line for line in text.splitlines() if "`igenomes_base`:" in line]
    assert lines == [
        '- `igenomes_base`: "s3://ngi-igenomes/igenomes" to "s3://ngi-igenomes/igenomes/" '
        "(not pinned by this service: the default changes on upgrade)"
    ]


def test_render_sop_marks_default_no_longer_stated():
    old = make_schema(p=param(name="p", type="string", default="x"))
    new = make_schema(p=param(name="p", type="string"))
    text = render_sop(new, {}, pipeline="demo", version="2.0", old_schema=old, old_version="1.0")
    assert '- `p`: "x" to none (no longer stated in the schema; check the new release\'s nextflow.config)' in text


def test_render_sop_marks_pinned_default_change():
    old_schema = load_schema(OLD_SCHEMA_PATH)
    new_schema = load_schema(NEW_SCHEMA_PATH)
    params = load_params(SERVICE_PARAMS_PATH)
    assert "skip_biotype_qc" in params
    text = render_sop(
        new_schema,
        params,
        pipeline="nf-core/rnaseq",
        version="3.27.0",
        old_schema=old_schema,
        old_version="3.14.0",
    )
    lines = [line for line in text.splitlines() if "`skip_biotype_qc`" in line and "to" in line]
    assert any("(pinned)" in line for line in lines)


def test_render_sop_no_changes_section_without_old_schema():
    schema = make_schema(p=param(name="p", type="string"))
    text = render_sop(schema, {}, pipeline="demo", version="1.0")
    assert "## Changes since" not in text


def test_render_sop_run_command_without_profile():
    schema = make_schema(p=param(name="p", type="string"))
    text = render_sop(schema, {}, pipeline="demo/pipeline", version="1.0")
    assert "nextflow run demo/pipeline -r 1.0 -params-file params.json" in text
    assert "-profile" not in text


def test_render_sop_run_command_with_profile():
    schema = make_schema(p=param(name="p", type="string"))
    text = render_sop(schema, {}, pipeline="demo/pipeline", version="1.0", profile="docker")
    assert "nextflow run demo/pipeline -r 1.0 -params-file params.json -profile docker" in text


def test_render_sop_table_cell_escaping():
    schema = make_schema(
        p=param(name="p", type="string", description="Uses a | pipe\nand a newline. Second sentence.")
    )
    text = render_sop(schema, {"p": "x"}, pipeline="demo", version="1.0")
    row_lines = [line for line in text.splitlines() if line.startswith("| p |")]
    assert len(row_lines) == 1
    row = row_lines[0]
    # the newline must have become a space, and the pipe must be escaped so the row stays one row.
    assert "\n" not in row
    assert "Uses a \\| pipe and a newline." in row
    assert "Uses a | pipe" not in row


def test_render_sop_checks_section_lists_issues():
    schema = make_schema(r=param(name="r", type="string", required=True))
    text = render_sop(schema, {}, pipeline="demo", version="1.0")
    assert "- error: `r`: required parameter is missing and has no default" in text


def test_render_sop_checks_section_no_problems():
    schema = make_schema(p=param(name="p", type="string"))
    text = render_sop(schema, {"p": "x"}, pipeline="demo", version="1.0")
    assert "No problems found." in text


def test_render_sop_required_inputs_lists_required_names():
    schema = make_schema(
        req=param(name="req", type="string", required=True, description="A required thing."),
        opt=param(name="opt", type="string", required=False),
    )
    text = render_sop(schema, {}, pipeline="demo", version="1.0")
    assert "- `req`: A required thing." in text
    assert "`opt`" not in text.split("## Required inputs")[1].split("## Checks")[0]


def test_render_sop_pipeline_defaults_excludes_hidden_and_set_params():
    schema = make_schema(
        visible_default=param(name="visible_default", type="string", default="x"),
        hidden_default=param(name="hidden_default", type="string", default="y", hidden=True),
        set_by_service=param(name="set_by_service", type="string", default="z"),
    )
    text = render_sop(schema, {"set_by_service": "z"}, pipeline="demo", version="1.0")
    defaults_section = text.split("## Pipeline defaults this service relies on")[1].split("## Required inputs")[0]
    assert "visible_default" in defaults_section
    assert "hidden_default" not in defaults_section
    assert "set_by_service" not in defaults_section


def test_render_sop_is_deterministic():
    old_schema = load_schema(OLD_SCHEMA_PATH)
    new_schema = load_schema(NEW_SCHEMA_PATH)
    params = load_params(SERVICE_PARAMS_PATH)
    first = render_sop(
        new_schema,
        params,
        pipeline="nf-core/rnaseq",
        version="3.27.0",
        profile="singularity",
        old_schema=old_schema,
        old_version="3.14.0",
    )
    second = render_sop(
        new_schema,
        params,
        pipeline="nf-core/rnaseq",
        version="3.27.0",
        profile="singularity",
        old_schema=old_schema,
        old_version="3.14.0",
    )
    assert first == second

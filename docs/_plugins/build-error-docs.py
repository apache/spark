"""
Generate a unified page of documentation for all error conditions.
"""
import json
import os
import re
from itertools import chain
from pathlib import Path
from textwrap import dedent

# To avoid adding new direct dependencies, we import from within mkdocs.
# This is not ideal as unrelated updates to mkdocs may break this script.
from mkdocs.structure.pages import markdown

THIS_DIR = Path(__file__).parent
SPARK_PROJECT_ROOT = THIS_DIR.parents[1]
DOCS_ROOT = SPARK_PROJECT_ROOT / "docs"
ERROR_CONDITIONS_PATH = (
    SPARK_PROJECT_ROOT / "common/utils/src/main/resources/error/error-conditions.json"
)


def assemble_message(message_parts):
    message = " ".join(message_parts)
    cleaned_message = re.sub(r"(<.*?>)", lambda x: f"`{x.group(1)}`", message)
    return markdown.markdown(cleaned_message)


def load_error_conditions(path):
    with open(path) as f:
        raw_error_conditions = json.load(f)
    error_conditions = dict()
    for name, details in raw_error_conditions.items():
        if name.startswith("_LEGACY_ERROR") or name.startswith("INTERNAL_ERROR"):
            continue
        if "subClass" in details:
            for sub_name in details["subClass"]:
                details["subClass"][sub_name]["message"] = (
                    assemble_message(details["subClass"][sub_name]["message"])
                )
        details["message"] = assemble_message(details["message"])
        error_conditions[name] = details
    return error_conditions


def anchor_name(condition_name: str, sub_condition_name: str = None):
    """
    URLs can, in practice, be up to 2,000 characters long without causing any issues. So we preserve
    the condition name mostly as-is for use in the anchor, even when that name is very long.
    See: https://stackoverflow.com/a/417184
    """
    parts = [
        part for part in (condition_name, sub_condition_name)
        if part
    ]
    anchor = "-".join(parts).lower().replace("_", "-")
    return anchor


def generate_doc_rows(condition_name, condition_details):
    condition_row = [
        """
        <tr id="{anchor}">
            <td>{sql_state}</td>
            <td>
                <span class="error-condition-name">
                    <code>
                    <a href="#{anchor}">#</a>
                    </code>
                    {condition_name}
                </span>
            </td>
            <td>{message}</td>
        </tr>
        """
        .format(
            anchor=anchor_name(condition_name),
            sql_state=condition_details["sqlState"],
            # This inserts soft break opportunities so that if a long name needs to be wrapped
            # it will wrap in a visually pleasing manner.
            # See: https://developer.mozilla.org/en-US/docs/Web/HTML/Element/wbr
            condition_name=condition_name.replace("_", "<wbr />_"),
            message=condition_details["message"],
        )
    ]
    sub_condition_rows = []
    if "subClass" in condition_details:
        for sub_condition_name in sorted(condition_details["subClass"]):
            sub_condition_rows.append(
                """
                <tr id="{anchor}">
                    <td></td>
                    <td class="error-sub-condition">
                        <span class="error-condition-name">
                            <code>
                            <a href="#{anchor}">#</a>
                            </code>
                            {sub_condition_name}
                        </span>
                    </td>
                    <td class="error-sub-condition">{message}</td>
                </tr>
                """
                .format(
                    anchor=anchor_name(condition_name, sub_condition_name),
                    # See comment above for explanation of `<wbr />`.
                    sub_condition_name=sub_condition_name.replace("_", "<wbr />_"),
                    message=condition_details["subClass"][sub_condition_name]["message"],
                )
            )
    doc_rows = condition_row + sub_condition_rows
    return [
        dedent(row).strip()
        for row in doc_rows
    ]


def generate_doc_table(error_conditions):
    doc_rows = chain.from_iterable([
        generate_doc_rows(condition_name, condition_details)
        for condition_name, condition_details
        in sorted(
            error_conditions.items(),
            key=lambda x: (x[1]["sqlState"], x[0]),
        )
    ])
    table_html = (
        """
        <table id="error-conditions">
        <tr>
            <th>Error State / SQLSTATE</th>
            <th>Error Condition & Sub-Condition</th>
            <th>Message</th>
        </tr>
        {rows}
        </table>
        """
    )
    # We dedent here rather than above so that the interpolated rows (which are not
    # indented) don't prevent the dedent from working.
    table_html = dedent(table_html).strip().format(rows="\n".join(list(doc_rows)))
    return table_html


if __name__ == "__main__":
    error_conditions = load_error_conditions(ERROR_CONDITIONS_PATH)
    doc_table = generate_doc_table(error_conditions)
    (DOCS_ROOT / "_generated").mkdir(exist_ok=True)
    html_table_path = DOCS_ROOT / "_generated" / "error-conditions.html"
    with open(html_table_path, "w") as f:
        f.write(doc_table)
    print("Generated:", os.path.relpath(html_table_path, start=SPARK_PROJECT_ROOT))

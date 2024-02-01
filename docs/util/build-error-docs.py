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
ERROR_CLASSES_PATH = SPARK_PROJECT_ROOT / "common/utils/src/main/resources/error/error-classes.json"


def assemble_message(message_parts):
    message = " ".join(message_parts)
    cleaned_message = re.sub(r"(<.*?>)", lambda x: f"`{x.group(1)}`", message)
    return markdown.markdown(cleaned_message)


def load_error_classes(path):
    with open(path) as f:
        raw_error_classes = json.load(f)
    error_classes = dict()
    for name, details in raw_error_classes.items():
        if name.startswith("_LEGACY_ERROR") or name.startswith("INTERNAL_ERROR"):
            continue
        if "subClass" in details:
            for sub_name in details["subClass"]:
                details["subClass"][sub_name]["message"] = assemble_message(details["subClass"][sub_name]["message"])
        details["message"] = assemble_message(details["message"])
        error_classes[name] = details
    return error_classes


def anchor_name(class_name: str, sub_class_name: str = None):
    """
    URLs can, in practice, be up to 2,000 characters long without causing any issues. So
    we preserve the class name most as-is for use in the anchor, even when that name is very
    long.
    See: https://stackoverflow.com/a/417184
    """
    parts = [
        part for part in (class_name, sub_class_name)
        if part
    ]
    anchor = "-".join(parts).lower().replace("_", "-")
    # machine_part = (
    #     hashlib.sha1(class_name.encode(), usedforsecurity=False)
    #     .hexdigest()
    #     .lower()[:4]
    # )
    return anchor


def generate_doc_rows(class_name, class_details):
    class_row = [
        """
        <tr id="{anchor}">
            <td>{sql_state}</td>
            <td>
            <!--<td colspan="2">-->
                <span class="error-name">
                    <code>
                    <a href="#{anchor}">#</a>
                    </code>
                    {class_name}
                </span>
            </td>
            <td>{message}</td>
            <!--<td colspan="2">{message}</td>-->
        </tr>
        """
        .format(
            anchor=anchor_name(class_name),
            sql_state=class_details["sqlState"],
            # This inserts soft break opportunities so that if a long name needs to be wrapped
            # it will wrap in a visually pleasing manner.
            # See: https://developer.mozilla.org/en-US/docs/Web/HTML/Element/wbr
            class_name=class_name.replace("_", "<wbr />_"),
            message=class_details["message"],
        )
    ]
    sub_class_rows = []
    if "subClass" in class_details:
        for sub_class_name in sorted(class_details["subClass"]):
            sub_class_rows.append(
                """
                <tr id="{anchor}">
                    <td></td>
                    <!--<td></td>-->
                    <td class="error-sub-class">
                        <span class="error-name">
                            <code>
                            <a href="#{anchor}">#</a>
                            </code>
                            {sub_class_name}
                        </span>
                    </td>
                    <!--<td></td>-->
                    <td class="error-sub-class">{message}</td>
                </tr>
                """
                .format(
                    anchor=anchor_name(class_name, sub_class_name),
                    # See comment above for explanation of `<wbr />`.
                    sub_class_name=sub_class_name.replace("_", "<wbr />_"),
                    message=class_details["subClass"][sub_class_name]["message"],
                )
            )
    doc_rows = class_row + sub_class_rows
    return [
        dedent(row).strip()
        for row in doc_rows
    ]


def generate_doc_table(error_classes):
    doc_rows = chain.from_iterable([
        generate_doc_rows(class_name, class_details)
        for class_name, class_details
        in sorted(
            error_classes.items(),
            key=lambda x: (x[1]["sqlState"], x[0]),
        )
    ])
    table_html = (
        """
        <table id="error-conditions">
        <tr>
            <th>Error State / SQLSTATE</th>
            <!--
            <th colspan="2">Error Condition & Sub-Condition</th>
            <th colspan="2">Message</th>
            -->
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
    error_classes = load_error_classes(ERROR_CLASSES_PATH)
    doc_table = generate_doc_table(error_classes)
    (DOCS_ROOT / "_generated").mkdir(exist_ok=True)
    html_table_path = DOCS_ROOT / "_generated" / "error-classes.html"
    with open(html_table_path, "w") as f:
        f.write(doc_table)
    print("Generated:", os.path.relpath(html_table_path, start=SPARK_PROJECT_ROOT))

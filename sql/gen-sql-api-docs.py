#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import itertools
import os
import re
from collections import namedtuple

from pyspark.java_gateway import launch_gateway


ExpressionInfo = namedtuple(
    "ExpressionInfo", "className name usage arguments examples note since deprecated group")


def _make_anchor(name):
    """
    Convert function name to a valid HTML anchor.
    Special characters are converted to descriptive names.

    Parameters:
    name (str): The function name.

    Returns:
    str: A valid HTML anchor string.
    """
    # Map special characters to descriptive names
    special_chars = {
        '!': 'not',
        '!=': 'notequal',
        '<>': 'notequal2',
        '<': 'lt',
        '<=': 'lte',
        '<=>': 'nullsafeequal',
        '=': 'eq',
        '==': 'equal',
        '>': 'gt',
        '>=': 'gte',
        '&': 'bitand',
        '|': 'bitor',
        '^': 'bitxor',
        '~': 'bitnot',
        '<<': 'shiftleft',
        '>>': 'shiftright',
        '>>>': 'shiftrightunsigned',
        '+': 'plus',
        '-': 'minus',
        '*': 'multiply',
        '/': 'divide',
        '%': 'mod',
        '||': 'concat',
    }

    if name in special_chars:
        return special_chars[name]

    # For regular names, convert to lowercase and replace spaces with hyphens
    # Remove any remaining special characters
    anchor = name.lower().replace(" ", "-")
    anchor = re.sub(r'[^a-z0-9_-]', '', anchor)
    return anchor


def _get_display_name(group):
    """
    Convert group name to display name.

    Parameters:
    group (str): The group name (e.g., "agg_funcs", "window_funcs").

    Returns:
    str: The display name (e.g., "Agg Functions", "Window Functions").
    """
    if group is None or group == "":
        return "Misc Functions"
    # Replace _funcs suffix, replace underscores with spaces, and title case
    name = group.replace("_funcs", "").replace("_", " ").title()
    return "%s Functions" % name


def _get_file_name(group):
    """
    Convert group name to file name.

    Parameters:
    group (str): The group name (e.g., "agg_funcs", "window_funcs", "operator").

    Returns:
    str: The file name (e.g., "agg-functions", "window-functions", "operator-functions").
    """
    if group is None or group == "":
        return "misc-functions"
    # Replace _funcs with -functions, replace underscores with hyphens
    file_name = group.replace("_funcs", "-functions").replace("_", "-")
    # If the group doesn't end with _funcs, append -functions
    if not group.endswith("_funcs") and not file_name.endswith("-functions"):
        file_name = file_name + "-functions"
    return file_name


# Groups that should be merged into other groups
GROUP_MERGES = {
    "lambda_funcs": "collection_funcs",  # SPARK-45232
}

_virtual_operator_infos = [
    ExpressionInfo(
        className="",
        name="!=",
        usage="expr1 != expr2 - Returns true if `expr1` is not equal to `expr2`, " +
              "or false otherwise.",
        arguments="\n    Arguments:\n      " +
                  """* expr1, expr2 - the two expressions must be same type or can be casted to
                       a common type, and must be a type that can be used in equality comparison.
                       Map type is not supported. For complex types such array/struct,
                       the data types of fields must be orderable.""",
        examples="\n    Examples:\n      " +
                 "> SELECT 1 != 2;\n      " +
                 " true\n      " +
                 "> SELECT 1 != '2';\n      " +
                 " true\n      " +
                 "> SELECT true != NULL;\n      " +
                 " NULL\n      " +
                 "> SELECT NULL != NULL;\n      " +
                 " NULL",
        note="",
        since="1.0.0",
        deprecated="",
        group="predicate_funcs"),
    ExpressionInfo(
        className="",
        name="<>",
        usage="expr1 != expr2 - Returns true if `expr1` is not equal to `expr2`, " +
              "or false otherwise.",
        arguments="\n    Arguments:\n      " +
                  """* expr1, expr2 - the two expressions must be same type or can be casted to
                       a common type, and must be a type that can be used in equality comparison.
                       Map type is not supported. For complex types such array/struct,
                       the data types of fields must be orderable.""",
        examples="\n    Examples:\n      " +
                 "> SELECT 1 != 2;\n      " +
                 " true\n      " +
                 "> SELECT 1 != '2';\n      " +
                 " true\n      " +
                 "> SELECT true != NULL;\n      " +
                 " NULL\n      " +
                 "> SELECT NULL != NULL;\n      " +
                 " NULL",
        note="",
        since="1.0.0",
        deprecated="",
        group="predicate_funcs"),
    ExpressionInfo(
        className="",
        name="case",
        usage="CASE expr1 WHEN expr2 THEN expr3 " +
              "[WHEN expr4 THEN expr5]* [ELSE expr6] END - " +
              "When `expr1` = `expr2`, returns `expr3`; " +
              "when `expr1` = `expr4`, return `expr5`; else return `expr6`.",
        arguments="\n    Arguments:\n      " +
                  "* expr1 - the expression which is one operand of comparison.\n      " +
                  "* expr2, expr4 - the expressions each of which is the other " +
                  "  operand of comparison.\n      " +
                  "* expr3, expr5, expr6 - the branch value expressions and else value expression" +
                  "  should all be same type or coercible to a common type.",
        examples="\n    Examples:\n      " +
                 "> SELECT CASE col1 WHEN 1 THEN 'one' " +
                 "WHEN 2 THEN 'two' ELSE '?' END FROM VALUES 1, 2, 3;\n      " +
                 " one\n      " +
                 " two\n      " +
                 " ?\n      " +
                 "> SELECT CASE col1 WHEN 1 THEN 'one' " +
                 "WHEN 2 THEN 'two' END FROM VALUES 1, 2, 3;\n      " +
                 " one\n      " +
                 " two\n      " +
                 " NULL",
        note="",
        since="1.0.1",
        deprecated="",
        group="conditional_funcs"),
    ExpressionInfo(
        className="",
        name="||",
        usage="expr1 || expr2 - Returns the concatenation of `expr1` and `expr2`.",
        arguments="",
        examples="\n    Examples:\n      " +
                 "> SELECT 'Spark' || 'SQL';\n      " +
                 " SparkSQL\n      " +
                 "> SELECT array(1, 2, 3) || array(4, 5) || array(6);\n      " +
                 " [1,2,3,4,5,6]",
        note="\n    || for arrays is available since 2.4.0.\n",
        since="2.3.0",
        deprecated="",
        group="string_funcs")
]


def _list_function_infos(jvm):
    """
    Returns a list of function information via JVM. Sorts wrapped expression infos by name
    and returns them.
    """

    jinfos = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listBuiltinFunctionInfos()
    infos = list(_virtual_operator_infos)  # Make a copy
    for jinfo in jinfos:
        name = jinfo.getName()
        usage = jinfo.getUsage()
        usage = usage.replace("_FUNC_", name) if usage is not None else usage
        # Get the group and apply any merges
        group = jinfo.getGroup()
        group = GROUP_MERGES.get(group, group)
        infos.append(ExpressionInfo(
            className=jinfo.getClassName(),
            name=name,
            usage=usage,
            arguments=jinfo.getArguments().replace("_FUNC_", name),
            examples=jinfo.getExamples().replace("_FUNC_", name),
            note=jinfo.getNote().replace("_FUNC_", name),
            since=jinfo.getSince(),
            deprecated=jinfo.getDeprecated(),
            group=group))
    return sorted(infos, key=lambda i: i.name)


def _list_grouped_function_infos(jvm):
    """
    Returns a list of function information grouped by category.
    Each item is a tuple of (group_key, list_of_infos).
    """
    infos = _list_function_infos(jvm)
    # Group by category
    grouped = itertools.groupby(
        sorted(infos, key=lambda x: (x.group or "", x.name)),
        key=lambda x: x.group
    )
    return [(k, list(g)) for k, g in grouped]


def _make_pretty_usage(usage):
    """
    Makes the usage description pretty and returns a formatted string if `usage`
    is not an empty string. Otherwise, returns None.
    """

    if usage is not None and usage.strip() != "":
        usage = "\n".join(map(lambda u: u.strip(), usage.split("\n")))
        return "%s\n\n" % usage


def _make_pretty_arguments(arguments):
    """
    Makes the arguments description pretty and returns a formatted string if `arguments`
    starts with the argument prefix. Otherwise, returns None.

    Expected input:

        Arguments:
          * arg0 - ...
              ...
          * arg0 - ...
              ...

    Expected output:
    **Arguments:**

    * arg0 - ...
        ...
    * arg0 - ...
        ...

    """

    if arguments.startswith("\n    Arguments:"):
        arguments = "\n".join(map(lambda u: u[6:], arguments.strip().split("\n")[1:]))
        return "**Arguments:**\n\n%s\n\n" % arguments


def _make_pretty_examples(examples):
    """
    Makes the examples description pretty and returns a formatted string if `examples`
    starts with the example prefix. Otherwise, returns None.

    Expected input:

        Examples:
          > SELECT ...;
           ...
          > SELECT ...;
           ...

    Expected output:
    **Examples:**

    ```
    > SELECT ...;
     ...
    > SELECT ...;
     ...
    ```

    """

    if examples.startswith("\n    Examples:"):
        examples = "\n".join(map(lambda u: u[6:], examples.strip().split("\n")[1:]))
        return "**Examples:**\n\n```\n%s\n```\n\n" % examples


def _make_pretty_note(note):
    """
    Makes the note description pretty and returns a formatted string if `note` is not
    an empty string. Otherwise, returns None.

    Expected input:

        ...

    Expected output:
    **Note:**

    ...

    """

    if note != "":
        note = "\n".join(map(lambda n: n[4:], note.split("\n")))
        return "**Note:**\n%s\n" % note


def _make_pretty_deprecated(deprecated):
    """
    Makes the deprecated description pretty and returns a formatted string if `deprecated`
    is not an empty string. Otherwise, returns None.

    Expected input:

        ...

    Expected output:
    **Deprecated:**

    ...

    """

    if deprecated != "":
        deprecated = "\n".join(map(lambda n: n[4:], deprecated.split("\n")))
        return "**Deprecated:**\n%s\n" % deprecated


def generate_sql_api_markdown(jvm, docs_dir):
    """
    Generates markdown files after listing the function information.
    Creates one file per category plus an index file.
    Also generates mkdocs.yml with auto-generated navigation.

    Expected output for each category file:
    # Category Name

    ### NAME

    USAGE

    **Arguments:**

    ARGUMENTS

    **Examples:**

    ```
    EXAMPLES
    ```

    **Note:**

    NOTE

    **Since:** SINCE

    **Deprecated:**

    DEPRECATED

    <br/>

    """

    def _write_function_entry(mdfile, info):
        """Write a single function entry to the markdown file."""
        name = info.name
        anchor = _make_anchor(name)
        usage = _make_pretty_usage(info.usage)
        arguments = _make_pretty_arguments(info.arguments)
        examples = _make_pretty_examples(info.examples)
        note = _make_pretty_note(info.note)
        since = info.since
        deprecated = _make_pretty_deprecated(info.deprecated)

        # Use explicit anchor for special characters
        mdfile.write('<a name="%s"></a>\n\n' % anchor)
        mdfile.write("### %s\n\n" % name)
        if usage is not None:
            mdfile.write("%s\n\n" % usage.strip())
        if arguments is not None:
            mdfile.write(arguments)
        if examples is not None:
            mdfile.write(examples)
        if note is not None:
            mdfile.write(note)
        if since is not None and since != "":
            mdfile.write("**Since:** %s\n\n" % since.strip())
        if deprecated is not None:
            mdfile.write(deprecated)
        mdfile.write("<br/>\n\n")

    # Group functions by category
    grouped_infos = _list_grouped_function_infos(jvm)

    # Track categories that have functions for the index
    categories_with_functions = []

    # Generate a separate markdown file for each category
    for group_key, infos in grouped_infos:
        display_name = _get_display_name(group_key)
        file_name = _get_file_name(group_key)
        categories_with_functions.append((group_key, display_name, file_name, len(infos)))

        category_path = os.path.join(docs_dir, "%s.md" % file_name)
        with open(category_path, 'w') as mdfile:
            mdfile.write("# %s\n\n" % display_name)
            mdfile.write("This page lists all %s available in Spark SQL.\n\n" % display_name.lower())
            mdfile.write("---\n\n")
            for info in infos:
                _write_function_entry(mdfile, info)

    # Generate the index file with links to all categories
    index_path = os.path.join(docs_dir, "index.md")
    with open(index_path, 'w') as mdfile:
        mdfile.write("# Built-in Functions\n\n")
        # Inline CSS for responsive grid layout
        css = """<style>
.func-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 0.5rem;
  margin-bottom: 1.5rem;
}
.func-grid a {
  padding: 0.25rem 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
@media (max-width: 1200px) {
  .func-grid { grid-template-columns: repeat(3, 1fr); }
}
@media (max-width: 768px) {
  .func-grid { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 480px) {
  .func-grid { grid-template-columns: 1fr; }
}
</style>

"""
        mdfile.write(css)
        mdfile.write("Spark SQL provides a comprehensive set of built-in functions for data ")
        mdfile.write("manipulation and analysis. Functions are organized into the following ")
        mdfile.write("categories:\n\n")

        # Sort categories by display name for consistent ordering
        sorted_categories = sorted(categories_with_functions, key=lambda x: x[1])

        # Create dictionary for efficient lookup
        grouped_dict = {k: infos for k, infos in grouped_infos}

        # Generate detailed TOC for each category with all function names
        for group_key, display_name, file_name, count in sorted_categories:
            mdfile.write("## %s (%d)\n\n" % (display_name, count))
            # Get the functions for this category
            category_infos = grouped_dict.get(group_key, [])
            # Write function links in a responsive grid layout
            mdfile.write('<div class="func-grid">\n')
            for info in category_infos:
                anchor = _make_anchor(info.name)
                mdfile.write('<a href="%s/#%s">%s</a>\n' % (file_name, anchor, info.name))
            mdfile.write('</div>\n\n')

    # Auto-generate mkdocs.yml with navigation
    _generate_mkdocs_yml(docs_dir, categories_with_functions)


def _generate_mkdocs_yml(docs_dir, categories_with_functions):
    """
    Generate mkdocs.yml with auto-generated navigation based on function categories.

    Parameters:
    docs_dir (str): The docs directory path.
    categories_with_functions (list): List of tuples (group_key, display_name, file_name, count).
    """
    # mkdocs.yml is in the parent directory of docs
    mkdocs_path = os.path.join(os.path.dirname(docs_dir), "mkdocs.yml")

    # Sort categories by display name for consistent ordering
    sorted_categories = sorted(categories_with_functions, key=lambda x: x[1])

    with open(mkdocs_path, 'w') as f:
        f.write("# AUTO-GENERATED FILE - DO NOT EDIT MANUALLY\n")
        f.write("# This file is generated by gen-sql-api-docs.py\n")
        f.write("# Run 'sql/create-docs.sh' to regenerate\n")
        f.write("\n")
        f.write("site_name: Spark SQL, Built-in Functions\n")
        f.write("theme:\n")
        f.write("  name: readthedocs\n")
        f.write("  navigation_depth: 3\n")
        f.write("  collapse_navigation: true\n")
        f.write("nav:\n")
        f.write("  - 'Overview': 'index.md'\n")

        for group_key, display_name, file_name, count in sorted_categories:
            f.write("  - '%s': '%s.md'\n" % (display_name, file_name))

        f.write("markdown_extensions:\n")
        f.write("  - toc:\n")
        f.write("      anchorlink: True\n")
        f.write("      permalink: True\n")
        f.write("  - tables\n")


if __name__ == "__main__":
    jvm = launch_gateway().jvm
    spark_root_dir = os.path.dirname(os.path.dirname(__file__))
    docs_dir = os.path.join(spark_root_dir, "sql/docs")
    # Create docs directory if it doesn't exist
    os.makedirs(docs_dir, exist_ok=True)
    generate_sql_api_markdown(jvm, docs_dir)

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

import os
import re
import sys
from collections import namedtuple
from textwrap import dedent

from mkdocs.structure.pages import markdown

ExpressionInfo = namedtuple(
    "ExpressionInfo", "className name usage arguments examples note since deprecated")
SQLConfEntry = namedtuple(
    "SQLConfEntry", ["name", "default", "docstring"])


def _list_function_infos(jvm):
    """
    Returns a list of function information via JVM. Sorts wrapped expression infos by name
    and returns them.
    """

    jinfos = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listBuiltinFunctionInfos()
    infos = []
    for jinfo in jinfos:
        name = jinfo.getName()
        usage = jinfo.getUsage()
        usage = usage.replace("_FUNC_", name) if usage is not None else usage
        infos.append(ExpressionInfo(
            className=jinfo.getClassName(),
            name=name,
            usage=usage,
            arguments=jinfo.getArguments().replace("_FUNC_", name),
            examples=jinfo.getExamples().replace("_FUNC_", name),
            note=jinfo.getNote(),
            since=jinfo.getSince(),
            deprecated=jinfo.getDeprecated()))
    return sorted(infos, key=lambda i: i.name)


def _list_sql_configs(jvm):
    sql_configs = [
        SQLConfEntry(
            name=_sql_config._1(),
            default=_sql_config._2(),
            docstring=_sql_config._3(),
        )
        for _sql_config in jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listSQLConfigs()
    ]
    return sql_configs


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


def generate_sql_markdown(jvm, path):
    """
    Generates a markdown file after listing the function information. The output file
    is created in `path`.

    Expected output:
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

    with open(path, 'w') as mdfile:
        for info in _list_function_infos(jvm):
            name = info.name
            usage = _make_pretty_usage(info.usage)
            arguments = _make_pretty_arguments(info.arguments)
            examples = _make_pretty_examples(info.examples)
            note = _make_pretty_note(info.note)
            since = info.since
            deprecated = _make_pretty_deprecated(info.deprecated)

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


def generate_sql_configs_table(jvm, path):
    """
    Generates an HTML table at `path` that lists all public SQL
    configuration options.
    """
    sql_configs = _list_sql_configs(jvm)
    value_reference_pattern = re.compile(r"^<value of (\S*)>$")
    # ConfigEntry(key=spark.buffer.size, defaultValue=65536, doc=, public=true)
    config_entry_pattern = re.compile(
        r"ConfigEntry\(key=(\S*), defaultValue=\S*, doc=\S*, public=\S*\)")

    with open(path, 'w') as f:
        f.write(dedent(
            """
            <table class="table">
            <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
            """
        ))
        for config in sorted(sql_configs, key=lambda x: x.name):
            if config.default == "<undefined>":
                default = "none"
            elif config.default.startswith("<value of "):
                referenced_config_name = value_reference_pattern.match(config.default).group(1)
                # difficultes in looking this up:
                #   a) potential recursion
                #   b) references to non-SQL configs
                default = "value of <code>{}</code>".format(referenced_config_name)
            else:
                default = config.default

            if default.startswith("<"):
                raise Exception(
                    "Unhandled reference in SQL config docs. Config '{name}' "
                    "has default '{default}' that looks like an HTML tag."
                    .format(
                        name=config.name,
                        default=config.default,
                    )
                )

            docstring = config_entry_pattern.sub(r"\g<1>", config.docstring)

            f.write(dedent(
                """
                <tr>
                    <td><code>{name}</code></td>
                    <td>{default}</td>
                    <td>{docstring}</td>
                </tr>
                """
                .format(
                    name=config.name,
                    default=default,
                    docstring=markdown.markdown(docstring),
                )
            ))
        f.write("</table>\n")


if __name__ == "__main__":
    from pyspark.java_gateway import launch_gateway

    jvm = launch_gateway().jvm
    spark_home = os.path.dirname(os.path.dirname(__file__))

    markdown_file_path = os.path.join(spark_home, "sql/docs/index.md")
    sql_configs_table_path = os.path.join(spark_home, "docs/_includes/sql-configs.html")

    generate_sql_markdown(jvm, markdown_file_path)
    generate_sql_configs_table(jvm, sql_configs_table_path)

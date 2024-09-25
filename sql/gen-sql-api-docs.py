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
from collections import namedtuple

from pyspark.java_gateway import launch_gateway


ExpressionInfo = namedtuple(
    "ExpressionInfo", "className name usage arguments examples note since deprecated")

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
        deprecated=""),
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
        deprecated=""),
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
        deprecated=""),
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
        deprecated="")
]


def _list_function_infos(jvm):
    """
    Returns a list of function information via JVM. Sorts wrapped expression infos by name
    and returns them.
    """

    jinfos = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listBuiltinFunctionInfos()
    infos = _virtual_operator_infos
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
            note=jinfo.getNote().replace("_FUNC_", name),
            since=jinfo.getSince(),
            deprecated=jinfo.getDeprecated()))
    return sorted(infos, key=lambda i: i.name)


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


def generate_sql_api_markdown(jvm, path):
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
        mdfile.write("# Built-in Functions\n\n")
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


if __name__ == "__main__":
    jvm = launch_gateway().jvm
    spark_root_dir = os.path.dirname(os.path.dirname(__file__))
    markdown_file_path = os.path.join(spark_root_dir, "sql/docs/index.md")
    generate_sql_api_markdown(jvm, markdown_file_path)

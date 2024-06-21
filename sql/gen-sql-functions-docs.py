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

# To avoid adding a new direct dependency, we import markdown from within mkdocs.
from mkdocs.structure.pages import markdown

from pyspark.java_gateway import launch_gateway


ExpressionInfo = namedtuple("ExpressionInfo", "name usage examples group")

groups = {
    "agg_funcs", "array_funcs", "datetime_funcs",
    "json_funcs", "map_funcs", "window_funcs",
    "math_funcs", "conditional_funcs", "generator_funcs",
    "predicate_funcs", "string_funcs", "misc_funcs",
    "bitwise_funcs", "conversion_funcs", "csv_funcs",
    "xml_funcs", "lambda_funcs", "collection_funcs",
    "url_funcs", "hash_funcs", "struct_funcs",
}


def _list_grouped_function_infos(jvm):
    """
    Returns a list of function information grouped by each group value via JVM.
    Sorts wrapped expression infos in each group by name and returns them.
    """

    jinfos = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listBuiltinFunctionInfos()
    infos = []

    for jinfo in filter(lambda x: x.getGroup() in groups, jinfos):
        name = jinfo.getName()
        if (name == "raise_error"):
            continue

        # SPARK-45232: convert lambda_funcs to collection_funcs in doc generation
        group = jinfo.getGroup()
        if group == "lambda_funcs":
            group = "collection_funcs"

        usage = jinfo.getUsage()
        usage = usage.replace("_FUNC_", name) if usage is not None else usage
        infos.append(ExpressionInfo(
            name=name,
            usage=usage,
            examples=jinfo.getExamples().replace("_FUNC_", name),
            group=group))

    # Groups expression info by each group value
    grouped_infos = itertools.groupby(sorted(infos, key=lambda x: x.group), key=lambda x: x.group)
    # Then, sort expression infos in each group by name
    return [(k, sorted(g, key=lambda x: x.name)) for k, g in grouped_infos]


# TODO(SPARK-31499): Needs to add a column to describe arguments and their types
def _make_pretty_usage(infos):
    """
    Makes the usage description pretty and returns a formatted string.

    Expected input:

        func(*) - ...

        func(expr[, expr...]) - ...

    Expected output:
    <table class="table">
      <thead>
        <tr>
          <th style="width:25%">Function</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>func(*)</td>
          <td>...</td>
        </tr>
        <tr>
          <td>func(expr[, expr...])</td>
          <td>...</td>
        </tr>
      </tbody>
      ...
    </table>

    """

    result = []
    result.append("<table class=\"table\">")
    result.append("  <thead>")
    result.append("    <tr>")
    result.append("      <th style=\"width:25%\">Function</th>")
    result.append("      <th>Description</th>")
    result.append("    </tr>")
    result.append("  </thead>")
    result.append("  <tbody>")

    for info in infos:
        # Extracts (signature, description) pairs from `info.usage`.
        # Expected formats are as follows;
        #  - `_FUNC_(...) - description`, or
        #  - `_FUNC_ - description`
        func_name = info.name
        if (info.name == "*" or info.name == "+"):
            func_name = "\\" + func_name
        elif (info.name == "when"):
            func_name = "CASE WHEN"
        usages = iter(re.split(r"(.*%s.*) - " % func_name, info.usage.strip())[1:])
        for (sig, description) in zip(usages, usages):
            result.append("    <tr>")
            result.append("      <td>%s</td>" % sig)
            result.append("      <td>%s</td>" % description.strip())
            result.append("    </tr>")

    result.append("  </tbody>")
    result.append("</table>\n")
    return "\n".join(result)


def _make_pretty_examples(jspark, infos):
    """
    Makes the examples description pretty and returns a formatted string if `infos`
    has any `examples` starting with the example prefix. Otherwise, returns None.

    Expected input:

        Examples:
          > SELECT func(col)...;
           ...
          > SELECT func(col)...;
           ...

    Expected output:
    <div class="codehilite"><pre><span></span>
      <span class="c1">-- func</span>
      <span class="k">SELECT</span>
      ...
    </pre></div>
    ```

    """

    pretty_output = ""
    for info in infos:
        if (info.examples.startswith("\n    Examples:")
                and info.name.lower() not in ("from_avro", "to_avro")):
            output = []
            output.append("-- %s" % info.name)
            query_examples = filter(lambda x: x.startswith("      > "), info.examples.split("\n"))
            for query_example in query_examples:
                query = query_example.lstrip("      > ")
                print("    %s" % query)
                query_output = jspark.sql(query).showString(20, 20, False)
                output.append(query)
                output.append(query_output)
            pretty_output += "\n" + "\n".join(output)
    if pretty_output != "":
        return markdown.markdown(
            "```sql%s```" % pretty_output, extensions=['codehilite', 'fenced_code'])


def generate_functions_table_html(jvm, html_output_dir):
    """
    Generates a HTML file after listing the function information. The output file
    is created under `html_output_dir`.

    Expected output:

    <table class="table">
      <thead>
        <tr>
          <th style="width:25%">Function</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>func(*)</td>
          <td>...</td>
        </tr>
        <tr>
          <td>func(expr[, expr...])</td>
          <td>...</td>
        </tr>
      </tbody>
      ...
    </table>

    """
    for key, infos in _list_grouped_function_infos(jvm):
        function_table = _make_pretty_usage(infos)
        key = key.replace("_", "-")
        with open("%s/generated-%s-table.html" % (html_output_dir, key), 'w') as table_html:
            table_html.write(function_table)


def generate_functions_examples_html(jvm, jspark, html_output_dir):
    """
    Generates a HTML file after listing and executing the function information.
    The output file is created under `html_output_dir`.

    Expected output:

    <div class="codehilite"><pre><span></span>
      <span class="c1">-- func</span>
      <span class="k">SELECT</span>
      ...
    </pre></div>

    """
    print("Running SQL examples to generate formatted output.")
    for key, infos in _list_grouped_function_infos(jvm):
        examples = _make_pretty_examples(jspark, infos)
        key = key.replace("_", "-")
        if examples is not None:
            with open("%s/generated-%s-examples.html" % (
                    html_output_dir, key), 'w') as examples_html:
                examples_html.write(examples)


if __name__ == "__main__":
    jvm = launch_gateway().jvm
    jspark = jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
    jspark.sparkContext().setLogLevel("ERROR")  # Make it less noisy.
    spark_root_dir = os.path.dirname(os.path.dirname(__file__))
    html_output_dir = os.path.join(spark_root_dir, "docs")
    generate_functions_table_html(jvm, html_output_dir)
    generate_functions_examples_html(jvm, jspark, html_output_dir)

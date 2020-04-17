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
from pyspark.sql import SparkSession

ExpressionInfo = namedtuple("ExpressionInfo", "name usage examples group")

markdown_header = \
    "---\n"\
    "layout: global\n"\
    "title: Built-in Functions\n"\
    "displayTitle: Built-in Functions\n"\
    "license: |\n"\
    "  Licensed to the Apache Software Foundation (ASF) under one or more\n"\
    "  contributor license agreements.  See the NOTICE file distributed with\n"\
    "  this work for additional information regarding copyright ownership.\n"\
    "  The ASF licenses this file to You under the Apache License, Version 2.0\n"\
    "  (the \"License\"); you may not use this file except in compliance with\n"\
    "  the License.  You may obtain a copy of the License at\n"\
    "\n"\
    "  http://www.apache.org/licenses/LICENSE-2.0\n"\
    "\n"\
    "  Unless required by applicable law or agreed to in writing, software\n"\
    "  distributed under the License is distributed on an \"AS IS\" BASIS,\n"\
    "  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"\
    "  See the License for the specific language governing permissions and\n"\
    "  limitations under the License.\n"\
    "---"

group_titles = {
    "agg_funcs": "Aggregate Functions",
    "array_funcs": "Array Functions",
    "datetime_funcs": "Date and Timestamp Functions",
    "json_funcs": "JSON Functions",
    "map_funcs": "Map Functions",
    "window_funcs": "Window Functions",
}


def _list_grouped_function_infos(jvm):
    """
    Returns a list of function information grouped by each group value via JVM.
    Sorts wrapped expression infos in each group by name and returns them.
    """

    jinfos = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listBuiltinFunctionInfos()
    expected_groups = group_titles.keys()
    infos = []

    for jinfo in filter(lambda x: x.getGroup() in expected_groups, jinfos):
        name = jinfo.getName()
        usage = jinfo.getUsage()
        usage = usage.replace("_FUNC_", name) if usage is not None else usage
        infos.append(ExpressionInfo(
            name=name,
            usage=usage,
            examples=jinfo.getExamples().replace("_FUNC_", name),
            group=jinfo.getGroup()))

    # Groups expression info by each group value
    grouped_infos = itertools.groupby(sorted(infos, key=lambda x: x.group), key=lambda x: x.group)
    # Then, sort expression infos in each group by name
    return [(k, sorted(g, key=lambda x: x.name)) for k, g in grouped_infos]


# TODO(maropu) Needs to add a column to describe arguments and their types
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
        # Extracts (signature, description) pairs from `info.usage`, e.g.,
        # the signature is `func(expr)` and the description is `...` in an usage `func(expr) - ...`.
        usages = iter(re.split(r"(%s\(.*\)) - " % info.name, info.usage.strip())[1:])
        for (sig, description) in zip(usages, usages):
            result.append("    <tr>")
            result.append("      <td>%s</td>" % sig)
            result.append("      <td>%s</td>" % description.strip())
            result.append("    </tr>")

    result.append("  </tbody>")
    result.append("</table>\n")
    return "\n".join(result)


def _make_pretty_query_example(jspark, query):
    result = []
    print("    %s" % query)
    query_output = jspark.sql(query).showString(20, 20, False)
    result.append(query)
    result.extend(map(lambda x: "  %s" % x, query_output.split("\n")))
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
    -- group_value
    SELECT func(col)...;
       +---------+
       |func(col)|
       +---------+
       |      ...|
       +---------+

    SELECT func(col)...;
       +---------+
       |func(col)|
       +---------+
       |      ...|
       +---------+

    """

    if any(info.examples.startswith("\n    Examples:") for info in infos):
        result = []
        result.append("\n#### Examples\n")
        result.append("{% highlight sql %}")

        for info in infos:
            result.append("-- %s" % info.name)
            query_examples = filter(lambda x: x.startswith("      > "), info.examples.split("\n"))
            for query_example in query_examples:
                query = query_example.lstrip("      > ")
                result.append(_make_pretty_query_example(jspark, query))

        result.append("{% endhighlight %}\n")
        return "\n".join(result)


def generate_sql_markdown(jvm, jspark, path):
    """
    Generates a markdown file after listing the function information. The output file
    is created in `path`.

    Expected output:
    ---
    layout: global
    title: Built-in Functions
    displayTitle: Built-in Functions
    license:
    ...
    ---

    ### Aggregate Functions

    <table class="table">
      ...
    </table>

    #### Examples

    {% hightlight sql %}
    ...
    {% endhighlight %}

    """

    with open(path, 'w') as mdfile:
        filename = os.path.basename(__file__)
        mdfile.write("%s\n\n" % markdown_header)
        mdfile.write("<!-- This file is automatically generated by `sql/%s`-->\n" % filename)

        print("Running a SQL example to generate output.")
        for key, infos in _list_grouped_function_infos(jvm):
            mdfile.write("\n### %s\n\n" % group_titles[key])
            function_table = _make_pretty_usage(infos)
            examples = _make_pretty_examples(jspark, infos)
            mdfile.write(function_table)
            if examples is not None:
                mdfile.write(examples)


if __name__ == "__main__":
    jvm = launch_gateway().jvm
    print("Initializing Spark Session to generate examples.")
    jspark = jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
    jspark.sparkContext().setLogLevel("ERROR")
    spark_root_dir = os.path.dirname(os.path.dirname(__file__))
    markdown_file_path = os.path.join(spark_root_dir, "docs/sql-ref-functions-builtin.md")
    generate_sql_markdown(jvm, jspark, markdown_file_path)

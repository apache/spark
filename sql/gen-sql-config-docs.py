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
from collections import namedtuple
from textwrap import dedent

# To avoid adding a new direct dependency, we import markdown from within mkdocs.
from mkdocs.structure.pages import markdown
from pyspark.java_gateway import launch_gateway

SQLConfEntry = namedtuple(
    "SQLConfEntry", ["name", "default", "description", "version"])


def get_public_sql_configs(jvm):
    sql_configs = [
        SQLConfEntry(
            name=_sql_config._1(),
            default=_sql_config._2(),
            description=_sql_config._3(),
            version=_sql_config._4()
        )
        for _sql_config in jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listSQLConfigs()
    ]
    return sql_configs


def generate_sql_configs_table(sql_configs, path):
    """
    Generates an HTML table at `path` that lists all public SQL
    configuration options.

    The table will look something like this:

    ```html
    <table class="table">
    <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>

    <tr>
        <td><code>spark.sql.adaptive.enabled</code></td>
        <td>false</td>
        <td><p>When true, enable adaptive query execution.</p></td>
        <td>2.1.0</td>
    </tr>

    ...

    </table>
    ```
    """
    value_reference_pattern = re.compile(r"^<value of (\S*)>$")

    with open(path, 'w') as f:
        f.write(dedent(
            """
            <table class="table">
            <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
            """
        ))
        for config in sorted(sql_configs, key=lambda x: x.name):
            if config.default == "<undefined>":
                default = "(none)"
            elif config.default.startswith("<value of "):
                referenced_config_name = value_reference_pattern.match(config.default).group(1)
                default = "(value of <code>{}</code>)".format(referenced_config_name)
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

            f.write(dedent(
                """
                <tr>
                    <td><code>{name}</code></td>
                    <td>{default}</td>
                    <td>{description}</td>
                    <td>{version}</td>
                </tr>
                """
                .format(
                    name=config.name,
                    default=default,
                    description=markdown.markdown(config.description),
                    version=config.version
                )
            ))
        f.write("</table>\n")


if __name__ == "__main__":
    jvm = launch_gateway().jvm
    sql_configs = get_public_sql_configs(jvm)

    spark_root_dir = os.path.dirname(os.path.dirname(__file__))
    sql_configs_table_path = os.path.join(spark_root_dir, "docs/sql-configs.html")

    generate_sql_configs_table(sql_configs, path=sql_configs_table_path)

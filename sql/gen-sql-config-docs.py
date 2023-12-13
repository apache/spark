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

import re

from collections import namedtuple, defaultdict
from pathlib import Path
from textwrap import dedent

# To avoid adding a new direct dependency, we import markdown from within mkdocs.
from mkdocs.structure.pages import markdown

from pyspark.java_gateway import launch_gateway

SPARK_PROJECT_ROOT = Path(__file__).parents[1]

SQLConfEntry = namedtuple(
    "SQLConfEntry", [
        "name",
        "default",
        "description",
        "version",
        "tags",
    ]
)


def get_sql_configs(jvm):
    """
    Get all public SQL configurations, grouped by their documentation tag.
    Note that these tags are created in `SQLConf.scala` via `ConfigBuilder.withTag()`.
    """
    sql_configs = defaultdict(
        list, {
            "__all": [],
            "__no_group": [],
        }
    )
    config_set = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listAllSQLConfigsWithTags()
    for raw_config in config_set:
        sql_config = SQLConfEntry(
            name=raw_config._1(),
            default=raw_config._2(),
            description=raw_config._3(),
            version=raw_config._4(),
            tags=list(raw_config._5()),
        )
        sql_configs["__all"].append(sql_config)
        if not sql_config.tags:
            sql_configs["__no_group"].append(sql_config)
        else:
            for tag in sql_config.tags:
                sql_configs[tag].append(sql_config)
    return sql_configs


def get_normalized_default(config):
    """
    Get the config's default value and normalize it so that:
        - Dynamic defaults that are set at runtime are mapped to a non-dynamic definition.
        - Other values that look like HTML tags are reformatted so they display properly.
    """
    value_reference_pattern = re.compile(r"^<value of (\S*)>$")

    if config.name == "spark.sql.session.timeZone":
        default = "(value of local timezone)"
    elif config.name == "spark.sql.warehouse.dir":
        default = "(value of <code>$PWD/spark-warehouse</code>)"
    elif config.default == "<undefined>":
        default = "(none)"
    elif config.default.startswith("<value of "):
        referenced_config_name = value_reference_pattern.match(config.default).group(1)
        default = "(value of <code>{}</code>)".format(referenced_config_name)
    else:
        default = config.default

    if default.startswith("<"):
        raise RuntimeError(
            "Unhandled reference in SQL config docs. Config '{name}' "
            "has default '{default}' that looks like an HTML tag."
            .format(
                name=config.name,
                default=config.default,
            )
        )

    return default


def generate_sql_configs_table_html(sql_configs, path, group):
    """
    Generates an HTML table at `path` that lists all public SQL
    configuration options.

    The table will look something like this:

    ```html
    <table class="table">
    <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>

    <tr>
        <td><code>spark.sql.adaptive.enabled</code></td>
        <td>true</td>
        <td><p>When true, enable adaptive query execution.</p></td>
        <td>1.6.0</td>
    </tr>

    ...

    </table>
    ```
    """
    with open(path, 'w') as f:
        f.write(dedent(
            """
            <table class="table">
            <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
            """
        ))
        for config in sorted(sql_configs, key=lambda x: x.name):
            f.write(dedent(
                """
                <tr id="{anchor}">
                    <td>
                        <span style="white-space:nowrap">
                            <a href="#{anchor}"><code>#</code></a>
                            <code>{name}</code>
                        </span>
                    </td>
                    <td>{default}</td>
                    <td>{description}</td>
                    <td>{version}</td>
                </tr>
                """
                .format(
                    # Making the group part of the anchor id ensures unique anchors
                    # even if a config happens to show up multiple times on a given page.
                    anchor=f"{config.name}-{group}",
                    name=config.name,
                    default=get_normalized_default(config),
                    description=markdown.markdown(config.description),
                    version=config.version,
                )
            ))
        f.write("</table>\n")


if __name__ == "__main__":
    jvm = launch_gateway().jvm
    generated_dir = SPARK_PROJECT_ROOT / "docs" / "_generated"
    generated_dir.mkdir(exist_ok=True)

    sql_configs = get_sql_configs(jvm)
    for group in sql_configs:
        html_table_path = generated_dir / f"generated-sql-config-table-{group}.html"
        generate_sql_configs_table_html(sql_configs[group], path=html_table_path, group=group)
        print(f"Generated: {html_table_path}")

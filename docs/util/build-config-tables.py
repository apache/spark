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
import os.path
import re
import shutil

from collections import namedtuple, defaultdict
from pathlib import Path
from textwrap import dedent

# To avoid adding new direct dependencies, we import from within mkdocs.
# This is not ideal as unrelated updates to mkdocs may break this script.
# If we clean up our Python dev dependency management story, we should a) add
# the appropriate direct dependencies, and b) migrate away from PyYAML to
# a library like StrictYAML that does not allow duplicate keys.
# See: https://hitchdev.com/strictyaml/why/duplicate-keys-disallowed/
from mkdocs.structure.pages import markdown
from mkdocs.utils.yaml import yaml

from pyspark.java_gateway import launch_gateway

SPARK_PROJECT_ROOT = Path(__file__).parents[2]
CONFIG_TABLES_DIR = SPARK_PROJECT_ROOT / "docs" / "_generated" / "config_tables"
CONFIG_GROUPS_PATH = Path(__file__).parent / "config-groups.yaml"
RESERVED_CONFIG_GROUPS = {"sql-runtime", "sql-static"}

ConfigEntry = namedtuple(
    "ConfigEntry", [
        "name",
        "default",
        "description",
        "version",
    ]
)


def get_all_configs(jvm):
    """
    Get all public Spark configurations.
    """
    all_configs = dict()
    sql_runtime_configs = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listRuntimeSQLConfigs()
    sql_static_configs = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listStaticSQLConfigs()
    raw_configs = itertools.chain(sql_runtime_configs, sql_static_configs)
    for raw_config in raw_configs:
        config_name = raw_config._1()
        config = ConfigEntry(
            name=raw_config._1(),
            default=raw_config._2(),
            description=raw_config._3(),
            version=raw_config._4(),
        )
        all_configs[config_name] = config
    return all_configs


def get_config_groups(jvm, config_groups_path):
    """
    Load the config groups defined at the provided path, and also generate some
    additional config groups that are automatically managed.
    """
    with open(config_groups_path) as f:
        _config_groups = yaml.safe_load(f)
    config_groups = defaultdict(set, {
        k: set(v) if v else set()
        for k, v in _config_groups.items()
    })

    bad_group_names = {
        group for group in config_groups
        if not re.fullmatch(r"[a-z0-9-]+", group)
    }
    if bad_group_names:
        raise ValueError(
            "Only lower case letters, digits, and dashes are allowed in group names. "
            f"The following group names are invalid: {', '.join(bad_group_names)}"
        )

    reserved_groups_used = config_groups.keys() & RESERVED_CONFIG_GROUPS
    if reserved_groups_used:
        raise ValueError(
            f"The config groups defined at '{config_groups_path}' include the following "
            f"group names which are reserved: {', '.join(reserved_groups_used)}"
        )

    sql_runtime_configs = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listRuntimeSQLConfigs()
    sql_static_configs = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listStaticSQLConfigs()

    # These groups are automatically generated, as opposed to user-defined.
    for (group, raw_configs) in [
        ("sql-runtime", sql_runtime_configs),
        ("sql-static", sql_static_configs),
    ]:
        for raw_config in raw_configs:
            config_name = raw_config._1()
            config_groups[group].add(config_name)

    return config_groups


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
        raise ValueError(
            f"Unhandled reference in config docs. Config '{config.name}' "
            f"has default '{config.default}' that looks like an HTML tag."
        )

    return default


def generate_config_table_html(group, configs, path):
    """
    Generates an HTML table at `path` for the provided configs.

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
        for config in sorted(configs, key=lambda x: x.name):
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


def generate_config_tables(config_groups, all_configs):
    for group in sorted(config_groups):
        html_table_path = CONFIG_TABLES_DIR / f"{group}.html"
        configs_in_group = []
        for config_name in config_groups[group]:
            if config_name not in all_configs:
                raise ValueError(
                    f"Could not find config '{config_name}'. Make sure it's typed "
                    "correctly and refers to a public configuration."
                )
            configs_in_group.append(all_configs[config_name])
        generate_config_table_html(
            group=group,
            configs=configs_in_group,
            path=html_table_path,
        )
        print("Generated:", os.path.relpath(html_table_path, start=SPARK_PROJECT_ROOT))


if __name__ == "__main__":
    shutil.rmtree(CONFIG_TABLES_DIR, ignore_errors=True)
    CONFIG_TABLES_DIR.mkdir(parents=True)
    jvm = launch_gateway().jvm
    all_configs = get_all_configs(jvm)
    config_groups = get_config_groups(jvm, CONFIG_GROUPS_PATH)
    generate_config_tables(config_groups, all_configs)

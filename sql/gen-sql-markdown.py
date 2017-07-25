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

import sys
import os
from collections import namedtuple

ExpressionInfo = namedtuple("ExpressionInfo", "className usage name extended")


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
        extended = jinfo.getExtended()
        extended = extended.replace("_FUNC_", name) if extended is not None else extended
        infos.append(ExpressionInfo(
            className=jinfo.getClassName(),
            usage=usage,
            name=name,
            extended=extended))
    return sorted(infos, key=lambda i: i.name)


def _make_pretty_usage(usage):
    """
    Makes the usage description pretty and returns a formatted string.
    Otherwise, returns None.
    """

    if usage is not None and usage.strip() != "":
        usage = "\n".join(map(lambda u: u.strip(), usage.split("\n")))
        return "%s\n\n" % usage


def _make_pretty_extended(extended):
    """
    Makes the extended description pretty and returns a formatted string.
    Otherwise, returns None.
    """

    if extended is not None and extended.strip() != "":
        extended = "\n".join(map(lambda u: u.strip(), extended.split("\n")))
        return "```%s```\n\n" % extended


def generate_sql_markdown(jvm, path):
    """
    Generates a markdown file after listing the function information. The output file
    is created in `path`.
    """

    with open(path, 'w') as mdfile:
        for info in _list_function_infos(jvm):
            mdfile.write("### %s\n\n" % info.name)
            usage = _make_pretty_usage(info.usage)
            extended = _make_pretty_extended(info.extended)
            if usage is not None:
                mdfile.write(usage)
            if extended is not None:
                mdfile.write(extended)


if __name__ == "__main__":
    from pyspark.java_gateway import launch_gateway

    jvm = launch_gateway().jvm
    markdown_file_path = "%s/docs/index.md" % os.path.dirname(sys.argv[0])
    generate_sql_markdown(jvm, markdown_file_path)

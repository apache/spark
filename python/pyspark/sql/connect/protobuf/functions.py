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

"""
A collections of builtin protobuf functions
"""

from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import Dict, Optional, TYPE_CHECKING

from pyspark.sql.protobuf import functions as PyProtobufFunctions

from pyspark.sql.connect.column import Column
from pyspark.sql.connect.functions.builtin import _invoke_function, _to_col, _options_to_col, lit

if TYPE_CHECKING:
    from pyspark.sql.connect._typing import ColumnOrName


def from_protobuf(
    data: "ColumnOrName",
    messageName: str,
    descFilePath: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    binaryDescriptorSet: Optional[bytes] = None,
) -> Column:
    binary_proto = None
    if binaryDescriptorSet is not None:
        binary_proto = binaryDescriptorSet
    elif descFilePath is not None:
        binary_proto = _read_descriptor_set_file(descFilePath)

    # TODO: simplify the code when _invoke_function() supports None as input.
    if binary_proto is not None:
        if options is None:
            return _invoke_function(
                "from_protobuf", _to_col(data), lit(messageName), lit(binary_proto)
            )
        else:
            return _invoke_function(
                "from_protobuf",
                _to_col(data),
                lit(messageName),
                lit(binary_proto),
                _options_to_col(options),
            )
    else:
        if options is None:
            return _invoke_function("from_protobuf", _to_col(data), lit(messageName))
        else:
            return _invoke_function(
                "from_protobuf", _to_col(data), lit(messageName), _options_to_col(options)
            )


from_protobuf.__doc__ = PyProtobufFunctions.from_protobuf.__doc__


def to_protobuf(
    data: "ColumnOrName",
    messageName: str,
    descFilePath: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    binaryDescriptorSet: Optional[bytes] = None,
) -> Column:
    binary_proto = None
    if binaryDescriptorSet is not None:
        binary_proto = binaryDescriptorSet
    elif descFilePath is not None:
        binary_proto = _read_descriptor_set_file(descFilePath)

    # TODO: simplify the code when _invoke_function() supports None as input.
    if binary_proto is not None:
        if options is None:
            return _invoke_function(
                "to_protobuf", _to_col(data), lit(messageName), lit(binary_proto)
            )
        else:
            return _invoke_function(
                "to_protobuf",
                _to_col(data),
                lit(messageName),
                lit(binary_proto),
                _options_to_col(options),
            )
    else:
        if options is None:
            return _invoke_function("to_protobuf", _to_col(data), lit(messageName))
        else:
            return _invoke_function(
                "to_protobuf", _to_col(data), lit(messageName), _options_to_col(options)
            )


to_protobuf.__doc__ = PyProtobufFunctions.to_protobuf.__doc__


def _read_descriptor_set_file(filePath: str) -> bytes:
    with open(filePath, "rb") as f:
        return f.read()


def _test() -> None:
    import os
    import sys
    from pyspark.testing.utils import search_jar

    protobuf_jar = search_jar("connector/protobuf", "spark-protobuf-assembly-", "spark-protobuf")
    if protobuf_jar is None:
        print(
            "Skipping all Protobuf Python tests as the optional Protobuf project was "
            "not compiled into a JAR. To run these tests, "
            "you need to build Spark with 'build/sbt package' or "
            "'build/mvn package' before running this test."
        )
        sys.exit(0)
    else:
        existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        jars_args = "--jars %s" % protobuf_jar
        os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join([jars_args, existing_args])

    import doctest
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.protobuf.functions

    globs = pyspark.sql.connect.protobuf.functions.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.protobuf.functions tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[2]"))
        .getOrCreate()
    )

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.connect.protobuf.functions,
        globs=globs,
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL,
    )

    globs["spark"].stop()

    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()

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


from typing import Dict, Optional, TYPE_CHECKING
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column
from pyspark.util import _print_missing_jar

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName


def from_protobuf(
    data: "ColumnOrName",
    descFilePath: str,
    messageName: str,
    options: Optional[Dict[str, str]] = None,
) -> Column:
    """
    Converts a binary column of Protobuf format into its corresponding catalyst value.
    The specified schema must match the read data, otherwise the behavior is undefined:
    it may fail or return arbitrary result.
    To deserialize the data with a compatible and evolved schema, the expected
    Protobuf schema can be set via the option protobuf descriptor.
    .. versionadded:: 3.4.0

    Parameters
    ----------
    data : :class:`~pyspark.sql.Column` or str
        the binary column.
    descFilePath : str
        the protobuf descriptor in Message GeneratedMessageV3 format.
    messageName: str
        the protobuf message name to look for in descriptorFile.
    options : dict, optional
        options to control how the Protobuf record is parsed.

    Notes
    -----
    Protobuf is built-in but external data source module since Spark 2.4. Please deploy the
    application as per the deployment section of "Protobuf Data Source Guide".

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> from pyspark.sql.types import *
    >>> from pyspark.sql.protobuf.functions import from_protobuf, to_protobuf
    >>> data = ([Row(key="1", value=Row(age=2, name="Alice", score=109200))])
    >>> schema = StructType([StructField("key", StringType(), False), \
    StructField( "value", StructType([ StructField("age", IntegerType(), False), \
    StructField("name", StringType(), False), StructField("score", LongType(), False), ]), False)])
    >>> df = spark.createDataFrame(data, schema)
    >>> descFilePath = 'connector/protobuf/src/test/resources/protobuf/pyspark_test.desc'
    >>> messageName = 'SimpleMessage'
    >>> protobufDf = df.select(to_protobuf(df.value, descFilePath, messageName).alias("protobuf"))
    >>> protobufDf.collect()
    [Row(protobuf=bytearray(b'\\x08\\x02\\x12\\x05Alice\\x18\\x90\\xd5\\x06'))]

    >>> descFilePath = 'connector/protobuf/src/test/resources/protobuf/pyspark_test.desc'
    >>> messageName = 'SimpleMessage'
    >>> df = protobufDf.select(from_protobuf(protobufDf.protobuf, \
    descFilePath, messageName).alias("value"))
    >>> df.collect()
    [Row(value=Row(age=2, name='Alice', score=109200))]
    """

    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    try:
        jc = sc._jvm.org.apache.spark.sql.protobuf.functions.from_protobuf(
            _to_java_column(data), descFilePath, messageName, options or {}
        )
    except TypeError as e:
        if str(e) == "'JavaPackage' object is not callable":
            _print_missing_jar("Protobuf", "protobuf", "protobuf", sc.version)
        raise
    return Column(jc)


def to_protobuf(data: "ColumnOrName", descFilePath: str, messageName: str) -> Column:
    """
    Converts a column into binary of protobuf format.

    .. versionadded:: 3.4.0

    Parameters
    ----------
    data : :class:`~pyspark.sql.Column` or str
        the data column.
    descFilePath : str
        the protobuf descriptor in Message GeneratedMessageV3 format.
    messageName: str
        the protobuf message name to look for in descriptorFile.

    Notes
    -----
    Protobuf is built-in but external data source module since Spark 2.4. Please deploy the
    application as per the deployment section of "Protobuf Data Source Guide".

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> from pyspark.sql.protobuf.functions import to_protobuf
    >>> from pyspark.sql.types import *
    >>> descFilePath = 'connector/protobuf/src/test/resources/protobuf/pyspark_test.desc'
    >>> messageName = 'SimpleMessage'
    >>> data = ([Row(value=Row(age=2, name="Alice", score=13093020))])
    >>> schema = StructType([StructField( "value", \
    StructType([ StructField("age", IntegerType(), False), \
    StructField("name", StringType(), False), \
    StructField("score", LongType(), False), ]), False)])
    >>> df = spark.createDataFrame(data, schema)
    >>> df.select(to_protobuf(df.value, descFilePath, messageName).alias("suite")).collect()
    [Row(suite=bytearray(b'\\x08\\x02\\x12\\x05Alice\\x18\\x9c\\x91\\x9f\\x06'))]
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    try:
        jc = sc._jvm.org.apache.spark.sql.protobuf.functions.to_protobuf(
            _to_java_column(data), descFilePath, messageName
        )
    except TypeError as e:
        if str(e) == "'JavaPackage' object is not callable":
            _print_missing_jar("Protobuf", "protobuf", "protobuf", sc.version)
        raise
    return Column(jc)


def _test() -> None:
    import os
    import sys
    from pyspark.testing.utils import search_jar

    protobuf_jar = search_jar("connector/protobuf", "spark-protobuf", "spark-protobuf")
    if protobuf_jar is None:
        print(
            "Skipping all Protobuf Python tests as the optional Protobuf project was "
            "not compiled into a JAR. To run these tests, "
            "you need to build Spark with 'build/sbt -Pprotobuf package' or "
            "'build/mvn -Pprotobuf package' before running this test."
        )
        sys.exit(0)
    else:
        existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        jars_args = "--jars %s" % protobuf_jar
        os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join([jars_args, existing_args])

    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.protobuf.functions

    globs = pyspark.sql.protobuf.functions.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("sql.protobuf.functions tests")
        .getOrCreate()
    )
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.protobuf.functions,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()

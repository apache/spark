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
A collections of builtin avro functions
"""


from typing import Dict, Optional, TYPE_CHECKING, cast

from pyspark.errors import PySparkTypeError
from pyspark.sql.column import Column
from pyspark.sql.utils import get_active_spark_context, try_remote_avro_functions
from pyspark.util import _print_missing_jar

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName


@try_remote_avro_functions
def from_avro(
    data: "ColumnOrName", jsonFormatSchema: str, options: Optional[Dict[str, str]] = None
) -> Column:
    """
    Converts a binary column of Avro format into its corresponding catalyst value.
    The specified schema must match the read data, otherwise the behavior is undefined:
    it may fail or return arbitrary result.
    To deserialize the data with a compatible and evolved schema, the expected Avro schema can be
    set via the option avroSchema.

    .. versionadded:: 3.0.0

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

    Parameters
    ----------
    data : :class:`~pyspark.sql.Column` or str
        the binary column.
    jsonFormatSchema : str
        the avro schema in JSON string format.
    options : dict, optional
        options to control how the Avro record is parsed.

    Notes
    -----
    Avro is built-in but external data source module since Spark 2.4. Please deploy the
    application as per the deployment section of "Apache Avro Data Source Guide".

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> from pyspark.sql.avro.functions import from_avro, to_avro
    >>> data = [(1, Row(age=2, name='Alice'))]
    >>> df = spark.createDataFrame(data, ("key", "value"))
    >>> avroDf = df.select(to_avro(df.value).alias("avro"))
    >>> avroDf.collect()
    [Row(avro=bytearray(b'\\x00\\x00\\x04\\x00\\nAlice'))]

    >>> jsonFormatSchema = '''{"type":"record","name":"topLevelRecord","fields":
    ...     [{"name":"avro","type":[{"type":"record","name":"value","namespace":"topLevelRecord",
    ...     "fields":[{"name":"age","type":["long","null"]},
    ...     {"name":"name","type":["string","null"]}]},"null"]}]}'''
    >>> avroDf.select(from_avro(avroDf.avro, jsonFormatSchema).alias("value")).collect()
    [Row(value=Row(avro=Row(age=2, name='Alice')))]
    """
    from py4j.java_gateway import JVMView
    from pyspark.sql.classic.column import _to_java_column

    if not isinstance(data, (Column, str)):
        raise PySparkTypeError(
            errorClass="INVALID_TYPE",
            messageParameters={
                "arg_name": "data",
                "arg_type": "pyspark.sql.Column or str",
            },
        )
    if not isinstance(jsonFormatSchema, str):
        raise PySparkTypeError(
            errorClass="INVALID_TYPE",
            messageParameters={"arg_name": "jsonFormatSchema", "arg_type": "str"},
        )
    if options is not None and not isinstance(options, dict):
        raise PySparkTypeError(
            errorClass="INVALID_TYPE",
            messageParameters={"arg_name": "options", "arg_type": "dict, optional"},
        )

    sc = get_active_spark_context()
    try:
        jc = cast(JVMView, sc._jvm).org.apache.spark.sql.avro.functions.from_avro(
            _to_java_column(data), jsonFormatSchema, options or {}
        )
    except TypeError as e:
        if str(e) == "'JavaPackage' object is not callable":
            _print_missing_jar("Avro", "avro", "avro", sc.version)
        raise
    return Column(jc)


@try_remote_avro_functions
def to_avro(data: "ColumnOrName", jsonFormatSchema: str = "") -> Column:
    """
    Converts a column into binary of avro format.

    .. versionadded:: 3.0.0

    .. versionchanged:: 3.5.0
        Supports Spark Connect.

    Parameters
    ----------
    data : :class:`~pyspark.sql.Column` or str
        the data column.
    jsonFormatSchema : str, optional
        user-specified output avro schema in JSON string format.

    Notes
    -----
    Avro is built-in but external data source module since Spark 2.4. Please deploy the
    application as per the deployment section of "Apache Avro Data Source Guide".

    Examples
    --------
    >>> from pyspark.sql import Row
    >>> from pyspark.sql.avro.functions import to_avro
    >>> data = ['SPADES']
    >>> df = spark.createDataFrame(data, "string")
    >>> df.select(to_avro(df.value).alias("suite")).collect()
    [Row(suite=bytearray(b'\\x00\\x0cSPADES'))]

    >>> jsonFormatSchema = '''["null", {"type": "enum", "name": "value",
    ...     "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}]'''
    >>> df.select(to_avro(df.value, jsonFormatSchema).alias("suite")).collect()
    [Row(suite=bytearray(b'\\x02\\x00'))]
    """
    from py4j.java_gateway import JVMView
    from pyspark.sql.classic.column import _to_java_column

    if not isinstance(data, (Column, str)):
        raise PySparkTypeError(
            errorClass="INVALID_TYPE",
            messageParameters={
                "arg_name": "data",
                "arg_type": "pyspark.sql.Column or str",
            },
        )
    if not isinstance(jsonFormatSchema, str):
        raise PySparkTypeError(
            errorClass="INVALID_TYPE",
            messageParameters={"arg_name": "jsonFormatSchema", "arg_type": "str"},
        )

    sc = get_active_spark_context()
    try:
        if jsonFormatSchema == "":
            jc = cast(JVMView, sc._jvm).org.apache.spark.sql.avro.functions.to_avro(
                _to_java_column(data)
            )
        else:
            jc = cast(JVMView, sc._jvm).org.apache.spark.sql.avro.functions.to_avro(
                _to_java_column(data), jsonFormatSchema
            )
    except TypeError as e:
        if str(e) == "'JavaPackage' object is not callable":
            _print_missing_jar("Avro", "avro", "avro", sc.version)
        raise
    return Column(jc)


def _test() -> None:
    import os
    import sys
    from pyspark.testing.utils import search_jar

    avro_jar = search_jar("connector/avro", "spark-avro", "spark-avro")
    if avro_jar is None:
        print(
            "Skipping all Avro Python tests as the optional Avro project was "
            "not compiled into a JAR. To run these tests, "
            "you need to build Spark with 'build/sbt -Pavro package' or "
            "'build/mvn -Pavro package' before running this test."
        )
        sys.exit(0)
    else:
        existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        jars_args = "--jars %s" % avro_jar
        os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join([jars_args, existing_args])

    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.avro.functions

    globs = pyspark.sql.avro.functions.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]").appName("sql.avro.functions tests").getOrCreate()
    )
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.avro.functions,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()

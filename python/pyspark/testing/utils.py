#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import glob
import os
import struct
import sys
import unittest
from time import time, sleep
from typing import (
    Any,
    Optional,
    Union,
    Dict,
    List,
    Tuple,
)
from itertools import zip_longest
import difflib

from pyspark import SparkContext, SparkConf
from pyspark.errors import PySparkAssertionError, PySparkException
from pyspark.find_spark_home import _find_spark_home
from pyspark.sql.dataframe import DataFrame as DataFrame
from pyspark.sql import Row
from pyspark.sql.types import StructType, AtomicType

have_scipy = False
have_numpy = False
try:
    import scipy.sparse  # noqa: F401

    have_scipy = True
except ImportError:
    # No SciPy, but that's okay, we'll skip those tests
    pass
try:
    import numpy as np  # noqa: F401

    have_numpy = True
except ImportError:
    # No NumPy, but that's okay, we'll skip those tests
    pass

__all__ = ["assertDataFrameEqual", "assertSchemaEqual"]

SPARK_HOME = _find_spark_home()


def read_int(b):
    return struct.unpack("!i", b)[0]


def write_int(i):
    return struct.pack("!i", i)


def eventually(condition, timeout=30.0, catch_assertions=False):
    """
    Wait a given amount of time for a condition to pass, else fail with an error.
    This is a helper utility for PySpark tests.

    Parameters
    ----------
    condition : function
        Function that checks for termination conditions. condition() can return:
            - True: Conditions met. Return without error.
            - other value: Conditions not met yet. Continue. Upon timeout,
              include last such value in error message.
              Note that this method may be called at any time during
              streaming execution (e.g., even before any results
              have been created).
    timeout : int
        Number of seconds to wait.  Default 30 seconds.
    catch_assertions : bool
        If False (default), do not catch AssertionErrors.
        If True, catch AssertionErrors; continue, but save
        error to throw upon timeout.
    """
    start_time = time()
    lastValue = None
    while time() - start_time < timeout:
        if catch_assertions:
            try:
                lastValue = condition()
            except AssertionError as e:
                lastValue = e
        else:
            lastValue = condition()
        if lastValue is True:
            return
        sleep(0.01)
    if isinstance(lastValue, AssertionError):
        raise lastValue
    else:
        raise AssertionError(
            "Test failed due to timeout after %g sec, with last condition returning: %s"
            % (timeout, lastValue)
        )


class QuietTest:
    def __init__(self, sc):
        self.log4j = sc._jvm.org.apache.log4j

    def __enter__(self):
        self.old_level = self.log4j.LogManager.getRootLogger().getLevel()
        self.log4j.LogManager.getRootLogger().setLevel(self.log4j.Level.FATAL)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.log4j.LogManager.getRootLogger().setLevel(self.old_level)


class PySparkTestCase(unittest.TestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.sc = SparkContext("local[4]", class_name)

    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path


class ReusedPySparkTestCase(unittest.TestCase):
    @classmethod
    def conf(cls):
        """
        Override this in subclasses to supply a more specific conf
        """
        return SparkConf()

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext("local[4]", cls.__name__, conf=cls.conf())

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


class ByteArrayOutput:
    def __init__(self):
        self.buffer = bytearray()

    def write(self, b):
        self.buffer += b

    def close(self):
        pass


def search_jar(project_relative_path, sbt_jar_name_prefix, mvn_jar_name_prefix):
    # Note that 'sbt_jar_name_prefix' and 'mvn_jar_name_prefix' are used since the prefix can
    # vary for SBT or Maven specifically. See also SPARK-26856
    project_full_path = os.path.join(SPARK_HOME, project_relative_path)

    # We should ignore the following jars
    ignored_jar_suffixes = ("javadoc.jar", "sources.jar", "test-sources.jar", "tests.jar")

    # Search jar in the project dir using the jar name_prefix for both sbt build and maven
    # build because the artifact jars are in different directories.
    sbt_build = glob.glob(
        os.path.join(project_full_path, "target/scala-*/%s*.jar" % sbt_jar_name_prefix)
    )
    maven_build = glob.glob(os.path.join(project_full_path, "target/%s*.jar" % mvn_jar_name_prefix))
    jar_paths = sbt_build + maven_build
    jars = [jar for jar in jar_paths if not jar.endswith(ignored_jar_suffixes)]

    if not jars:
        return None
    elif len(jars) > 1:
        raise RuntimeError("Found multiple JARs: %s; please remove all but one" % (", ".join(jars)))
    else:
        return jars[0]


class PySparkErrorTestUtils:
    """
    This util provide functions to accurate and consistent error testing
    based on PySpark error classes.
    """

    def check_error(
        self,
        exception: PySparkException,
        error_class: str,
        message_parameters: Optional[Dict[str, str]] = None,
    ):
        # Test if given error is an instance of PySparkException.
        self.assertIsInstance(
            exception,
            PySparkException,
            f"checkError requires 'PySparkException', got '{exception.__class__.__name__}'.",
        )

        # Test error class
        expected = error_class
        actual = exception.getErrorClass()
        self.assertEqual(
            expected, actual, f"Expected error class was '{expected}', got '{actual}'."
        )

        # Test message parameters
        expected = message_parameters
        actual = exception.getMessageParameters()
        self.assertEqual(
            expected, actual, f"Expected message parameters was '{expected}', got '{actual}'"
        )


def assertSchemaEqual(actual: StructType, expected: StructType):
    """
    A util function to assert equality between DataFrame schemas `actual` and `expected`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    actual : StructType
        The DataFrame schema that is being compared or tested.
    expected : StructType
        The expected schema, for comparison with the actual schema.

    Examples
    --------
    >>> from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, DoubleType
    >>> s1 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
    >>> s2 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
    >>> assertSchemaEqual(s1, s2)

    Pass, schemas are identical

    >>> s1 = StructType([StructField("names", ArrayType(IntegerType(), True), True)])
    >>> s2 = StructType([StructField("names", ArrayType(DoubleType(), False), True)])
    >>> assertSchemaEqual(s1, s2)  # fail  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    PySparkAssertionError: [DIFFERENT_SCHEMA] Schemas do not match:
    [df]
    StructField("names", ArrayType(IntegerType(), True), True)

    [expected]
    StructField("names", ArrayType(DoubleType(), False), True)
    """
    if not isinstance(actual, StructType):
        raise PySparkAssertionError(
            error_class="UNSUPPORTED_DATA_TYPE",
            message_parameters={"data_type": type(actual)},
        )
    if not isinstance(expected, StructType):
        raise PySparkAssertionError(
            error_class="UNSUPPORTED_DATA_TYPE",
            message_parameters={"data_type": type(expected)},
        )

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("assertSchemaEqual").getOrCreate()

    def compare_schemas_ignore_nullable(s1, s2):
        if len(s1) != len(s2):
            return False
        zipped = zip_longest(s1, s2)
        for sf1, sf2 in zipped:
            if not compare_structfields_ignore_nullable(sf1, sf2):
                return False
        return True

    def compare_structfields_ignore_nullable(actualSF, expectedSF):
        if actualSF is None and expectedSF is None:
            return True
        elif actualSF is None or expectedSF is None:
            return False
        if actualSF.name != expectedSF.name:
            return False
        else:
            return compare_datatypes_ignore_nullable(actualSF.dataType, expectedSF.dataType)

    def compare_datatypes_ignore_nullable(dt1, dt2):
        # checks datatype equality, using recursion to ignore nullable
        if dt1.typeName() == dt2.typeName():
            if dt1.typeName() == "array":
                return compare_datatypes_ignore_nullable(dt1.elementType, dt2.elementType)
            elif dt1.typeName() == "struct":
                return compare_schemas_ignore_nullable(dt1, dt2)
            else:
                return True
        else:
            return False

    if not compare_schemas_ignore_nullable(actual, expected):
        actual_schema_tree = spark.createDataFrame([], actual)._jdf.schema().treeString()
        expected_schema_tree = spark.createDataFrame([], expected)._jdf.schema().treeString()
        actual_schema_lst = actual_schema_tree.splitlines()
        expected_schema_lst = expected_schema_tree.splitlines()

        generated_diff = difflib.ndiff(actual_schema_lst, expected_schema_lst)

        error_msg = "\n".join(generated_diff)

        raise PySparkAssertionError(
            error_class="DIFFERENT_SCHEMA",
            message_parameters={"error_msg": error_msg},
        )


def assertDataFrameEqual(
    df: DataFrame,
    expected: Union[DataFrame, List[Row]],
    checkRowOrder: bool = False,
    rtol: float = 1e-5,
    atol: float = 1e-8,
):
    """
    A util function to assert equality between `df` (DataFrame) and `expected`
    (either DataFrame or list of Rows), with optional parameter `checkRowOrder`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    df : DataFrame
        The DataFrame that is being compared or tested.
    expected : DataFrame or list of Rows
        The expected result of the operation, for comparison with the actual result.
    checkRowOrder : bool, optional
        A flag indicating whether the order of rows should be considered in the comparison.
        If set to `False` (default), the row order is not taken into account.
        If set to `True`, the order of rows is important and will be checked during comparison.
        (See Notes)
    rtol : float, optional
        The relative tolerance, used in asserting approximate equality for float values in df
        and expected. Set to 1e-5 by default. (See Notes)
    atol : float, optional
        The absolute tolerance, used in asserting approximate equality for float values in df
        and expected. Set to 1e-8 by default. (See Notes)

    Notes
    -----
    For checkRowOrder, note that PySpark DataFrame ordering is non-deterministic, unless
    explicitly sorted.

    For DataFrames with float values, assertDataFrame asserts approximate equality.
    Two float values a and b are approximately equal if the following equation is True:

    ``absolute(a - b) <= (atol + rtol * absolute(b))``.

    Examples
    --------
    >>> df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    >>> df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    >>> assertDataFrameEqual(df1, df2) # pass
    >>> df1 = spark.createDataFrame(data=[("1", 1000.00), ("2", 3000.00), ("3", 2000.00)],
    ... schema=["id", "amount"])
    >>> df2 = spark.createDataFrame(data=[("1", 1001.00), ("2", 3000.00), ("3", 2003.00)],
    ... schema=["id", "amount"])
    >>> assertDataFrameEqual(df1, df2) # fail  # doctest: +IGNORE_EXCEPTION_DETAIL
    >>> assertDataFrameEqual(df1, df2)

    Pass, DataFrames are identical

    >>> df1 = spark.createDataFrame(data=[("1", 0.1), ("2", 3.23)], schema=["id", "amount"])
    >>> df2 = spark.createDataFrame(data=[("1", 0.109), ("2", 3.23)], schema=["id", "amount"])
    >>> assertDataFrameEqual(df1, df2, rtol=1e-1)

    Pass, DataFrames are approx equal by rtol

    >>> df1 = spark.createDataFrame(
    ...     data=[("1", 1000.00), ("2", 3000.00), ("3", 2000.00)], schema=["id", "amount"])
    >>> df2 = spark.createDataFrame(
    ...     data=[("1", 1001.00), ("2", 3000.00), ("3", 2003.00)], schema=["id", "amount"])
    >>> assertDataFrameEqual(df1, df2)  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    PySparkAssertionError: [DIFFERENT_ROWS] Results do not match: ( 66.667 % )
    [df]
    Row(id='1', amount=1000.0)

    [expected]
    Row(id='1', amount=1001.0)

    [df]
    Row(id='3', amount=2000.0)

    [expected]
    Row(id='3', amount=2003.0)
    """
    if df is None and expected is None:
        return True
    elif df is None or expected is None:
        return False

    try:
        # If Spark Connect dependencies are available, allow Spark Connect DataFrame
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        if not isinstance(df, DataFrame) and not isinstance(df, ConnectDataFrame):
            raise PySparkAssertionError(
                error_class="UNSUPPORTED_DATA_TYPE",
                message_parameters={"data_type": type(df)},
            )
        elif (
            not isinstance(expected, DataFrame)
            and not isinstance(expected, ConnectDataFrame)
            and not isinstance(expected, List)
        ):
            raise PySparkAssertionError(
                error_class="UNSUPPORTED_DATA_TYPE",
                message_parameters={"data_type": type(expected)},
            )
    except Exception:
        if not isinstance(df, DataFrame):
            raise PySparkAssertionError(
                error_class="UNSUPPORTED_DATA_TYPE",
                message_parameters={"data_type": type(df)},
            )
        elif not isinstance(expected, DataFrame) and not isinstance(expected, List):
            raise PySparkAssertionError(
                error_class="UNSUPPORTED_DATA_TYPE",
                message_parameters={"data_type": type(expected)},
            )

    # special cases: empty datasets, datasets with 0 columns
    if (df.first() is None and expected.first() is None) or (
        len(df.columns) == 0 and len(expected.columns) == 0
    ):
        return True

    def compare_rows(r1: Row, r2: Row):
        def compare_vals(val1, val2):
            if isinstance(val1, list) and isinstance(val2, list):
                return len(val1) == len(val2) and all(
                    compare_vals(x, y) for x, y in zip(val1, val2)
                )
            elif isinstance(val1, Row) and isinstance(val2, Row):
                return all(compare_vals(x, y) for x, y in zip(val1, val2))
            elif isinstance(val1, dict) and isinstance(val2, dict):
                return (
                    len(val1.keys()) == len(val2.keys())
                    and val1.keys() == val2.keys()
                    and all(compare_vals(val1[k], val2[k]) for k in val1.keys())
                )
            elif isinstance(val1, float) and isinstance(val2, float):
                if abs(val1 - val2) > (atol + rtol * abs(val2)):
                    return False
            else:
                if val1 != val2:
                    return False
            return True

        if r1 is None and r2 is None:
            return True
        elif r1 is None or r2 is None:
            return False

        return compare_vals(r1, r2)

    def assert_rows_equal(rows1: List[Row], rows2: List[Row]):
        zipped = list(zip_longest(rows1, rows2))
        rows_equal = True
        error_msg = "Results do not match: "
        diff_msg = ""
        diff_rows_cnt = 0

        for r1, r2 in zipped:
            if not compare_rows(r1, r2):
                rows_equal = False
                diff_rows_cnt += 1
                diff_msg += (
                    "[df]" + "\n" + str(r1) + "\n\n" + "[expected]" + "\n" + str(r2) + "\n\n"
                )
                diff_msg += "********************" + "\n\n"

        if not rows_equal:
            percent_diff = (diff_rows_cnt / len(zipped)) * 100
            error_msg += "( %.5f %% )" % percent_diff
            error_msg += "\n" + diff_msg
            raise PySparkAssertionError(
                error_class="DIFFERENT_ROWS",
                message_parameters={"error_msg": error_msg},
            )

    # convert df and expected to list
    if not isinstance(expected, List):
        # only compare schema if expected is not a List
        assertSchemaEqual(df.schema, expected.schema)
        expected_list = expected.collect()
    else:
        expected_list = expected

    df_list = df.collect()

    if not checkRowOrder:
        # rename duplicate columns for sorting
        df_list = sorted(df_list, key=lambda x: str(x))
        expected_list = sorted(expected_list, key=lambda x: str(x))

    assert_rows_equal(df_list, expected_list)


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.testing.utils

    globs = pyspark.testing.utils.__dict__.copy()
    spark = SparkSession.builder.master("local[4]").appName("testing.utils tests").getOrCreate()
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.testing.utils,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()

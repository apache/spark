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
import unittest
import datetime
import decimal
from typing import List

import pandas
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np

from pyspark.loose_version import LooseVersion
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    ByteType,
    ShortType,
    DateType,
    DecimalType,
    DoubleType,
    TimestampType,
)

from pyspark.pandas.typedef import (
    as_spark_type,
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
    infer_return_type,
    pandas_on_spark_type,
)
from pyspark import pandas as ps


class TypeHintTestsMixin:
    def test_infer_schema_with_no_return(self):
        def try_infer_return_type():
            def f():
                pass

            infer_return_type(f)

        self.assertRaisesRegex(
            ValueError, "A return value is required for the input function", try_infer_return_type
        )

        def try_infer_return_type():
            def f() -> None:
                pass

            infer_return_type(f)

        self.assertRaisesRegex(
            TypeError, "Type <class 'NoneType'> was not understood", try_infer_return_type
        )

    def test_infer_schema_from_pandas_instances(self):
        def func() -> pd.Series[int]:
            pass

        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtype, np.int64)
        self.assertEqual(inferred.spark_type, LongType())

        def func() -> pd.Series[float]:
            pass

        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtype, np.float64)
        self.assertEqual(inferred.spark_type, DoubleType())

        def func() -> "pd.DataFrame[np.float64, str]":
            pass

        expected = StructType([StructField("c0", DoubleType()), StructField("c1", StringType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.float64, np.str_])
        self.assertEqual(inferred.spark_type, expected)

        def func() -> "pandas.DataFrame[float]":
            pass

        expected = StructType([StructField("c0", DoubleType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.float64])
        self.assertEqual(inferred.spark_type, expected)

        def func() -> "pd.Series[int]":
            pass

        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtype, np.int64)
        self.assertEqual(inferred.spark_type, LongType())

        def func() -> pd.DataFrame[np.float64, str]:
            pass

        expected = StructType([StructField("c0", DoubleType()), StructField("c1", StringType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.float64, np.str_])
        self.assertEqual(inferred.spark_type, expected)

        def func() -> pd.DataFrame[np.float64]:
            pass

        expected = StructType([StructField("c0", DoubleType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.float64])
        self.assertEqual(inferred.spark_type, expected)

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [3, 4, 5]})

        def func() -> pd.DataFrame[pdf.dtypes]:
            pass

        expected = StructType([StructField("c0", LongType()), StructField("c1", LongType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.int64, np.int64])
        self.assertEqual(inferred.spark_type, expected)

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": pd.Categorical(["a", "b", "c"])})

        def func() -> pd.Series[pdf.b.dtype]:
            pass

        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtype, CategoricalDtype(categories=["a", "b", "c"]))
        self.assertEqual(inferred.spark_type, LongType())

        def func() -> pd.DataFrame[pdf.dtypes]:
            pass

        expected = StructType([StructField("c0", LongType()), StructField("c1", LongType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.int64, CategoricalDtype(categories=["a", "b", "c"])])
        self.assertEqual(inferred.spark_type, expected)

    def test_if_pandas_implements_class_getitem(self):
        # the current type hint implementation of pandas DataFrame assumes pandas doesn't
        # implement '__class_getitem__'. This test case is to make sure pandas
        # doesn't implement them.
        assert not ps._frame_has_class_getitem
        assert not ps._series_has_class_getitem

    def test_infer_schema_with_names_pandas_instances(self):
        def func() -> 'pd.DataFrame["a" : np.float64, "b":str]':  # noqa: F405
            pass

        expected = StructType([StructField("a", DoubleType()), StructField("b", StringType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.float64, np.str_])
        self.assertEqual(inferred.spark_type, expected)

        def func() -> "pd.DataFrame['a': float, 'b': int]":  # noqa: F405
            pass

        expected = StructType([StructField("a", DoubleType()), StructField("b", LongType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.float64, np.int64])
        self.assertEqual(inferred.spark_type, expected)

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [3, 4, 5]})

        def func() -> pd.DataFrame[zip(pdf.columns, pdf.dtypes)]:
            pass

        expected = StructType([StructField("a", LongType()), StructField("b", LongType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.int64, np.int64])
        self.assertEqual(inferred.spark_type, expected)

        pdf = pd.DataFrame({("x", "a"): [1, 2, 3], ("y", "b"): [3, 4, 5]})

        def func() -> pd.DataFrame[zip(pdf.columns, pdf.dtypes)]:
            pass

        expected = StructType(
            [StructField("(x, a)", LongType()), StructField("(y, b)", LongType())]
        )
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.int64, np.int64])
        self.assertEqual(inferred.spark_type, expected)

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": pd.Categorical(["a", "b", "c"])})

        def func() -> pd.DataFrame[zip(pdf.columns, pdf.dtypes)]:
            pass

        expected = StructType([StructField("a", LongType()), StructField("b", LongType())])
        inferred = infer_return_type(func)
        self.assertEqual(inferred.dtypes, [np.int64, CategoricalDtype(categories=["a", "b", "c"])])
        self.assertEqual(inferred.spark_type, expected)

    def test_infer_schema_with_names_pandas_instances_negative(self):
        def try_infer_return_type():
            def f() -> 'pd.DataFrame["a" : np.float64 : 1, "b":str:2]':  # noqa: F405
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "Type hints should be specified", try_infer_return_type)

        class A:
            pass

        def try_infer_return_type():
            def f() -> pd.DataFrame[A]:
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "not understood", try_infer_return_type)

        def try_infer_return_type():
            def f() -> 'pd.DataFrame["a" : float : 1, "b":str:2]':  # noqa: F405
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "Type hints should be specified", try_infer_return_type)

        # object type
        pdf = pd.DataFrame({"a": ["a", 2, None]})

        def try_infer_return_type():
            def f() -> pd.DataFrame[pdf.dtypes]:
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "object.*not understood", try_infer_return_type)

        def try_infer_return_type():
            def f() -> pd.Series[pdf.a.dtype]:
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "object.*not understood", try_infer_return_type)

    def test_infer_schema_with_names_negative(self):
        def try_infer_return_type():
            def f() -> 'ps.DataFrame["a" : float : 1, "b":str:2]':  # noqa: F405
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "Type hints should be specified", try_infer_return_type)

        class A:
            pass

        def try_infer_return_type():
            def f() -> ps.DataFrame[A]:
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "not understood", try_infer_return_type)

        def try_infer_return_type():
            def f() -> 'ps.DataFrame["a" : np.float64 : 1, "b":str:2]':  # noqa: F405
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "Type hints should be specified", try_infer_return_type)

        # object type
        pdf = pd.DataFrame({"a": ["a", 2, None]})

        def try_infer_return_type():
            def f() -> ps.DataFrame[pdf.dtypes]:
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "object.*not understood", try_infer_return_type)

        def try_infer_return_type():
            def f() -> ps.Series[pdf.a.dtype]:
                pass

            infer_return_type(f)

        self.assertRaisesRegex(TypeError, "object.*not understood", try_infer_return_type)

    def test_as_spark_type_pandas_on_spark_dtype(self):
        type_mapper = {
            # binary
            np.character: (np.character, BinaryType()),
            np.bytes_: (np.bytes_, BinaryType()),
            bytes: (np.bytes_, BinaryType()),
            # integer
            np.int8: (np.int8, ByteType()),
            np.byte: (np.int8, ByteType()),
            np.int16: (np.int16, ShortType()),
            np.int32: (np.int32, IntegerType()),
            np.int64: (np.int64, LongType()),
            int: (np.int64, LongType()),
            # floating
            np.float32: (np.float32, FloatType()),
            np.float64: (np.float64, DoubleType()),
            float: (np.float64, DoubleType()),
            # string
            np.str_: (np.str_, StringType()),
            str: (np.str_, StringType()),
            # bool
            bool: (np.bool_, BooleanType()),
            # datetime
            np.datetime64: (np.datetime64, TimestampType()),
            datetime.datetime: (np.dtype("datetime64[ns]"), TimestampType()),
            # DateType
            datetime.date: (np.dtype("object"), DateType()),
            # DecimalType
            decimal.Decimal: (np.dtype("object"), DecimalType(38, 18)),
            # ArrayType
            np.ndarray: (np.dtype("object"), ArrayType(StringType())),
            # CategoricalDtype
            CategoricalDtype(categories=["a", "b", "c"]): (
                CategoricalDtype(categories=["a", "b", "c"]),
                LongType(),
            ),
        }

        for numpy_or_python_type, (dtype, spark_type) in type_mapper.items():
            self.assertEqual(as_spark_type(numpy_or_python_type), spark_type)
            self.assertEqual(pandas_on_spark_type(numpy_or_python_type), (dtype, spark_type))

            if isinstance(numpy_or_python_type, CategoricalDtype):
                # Nested CategoricalDtype is not yet supported.
                continue

            self.assertEqual(as_spark_type(List[numpy_or_python_type]), ArrayType(spark_type))
            self.assertEqual(
                pandas_on_spark_type(List[numpy_or_python_type]),
                (np.dtype("object"), ArrayType(spark_type)),
            )

            # For NumPy typing, NumPy version should be 1.21+
            if LooseVersion(np.__version__) >= LooseVersion("1.21"):
                import numpy.typing as ntp

                self.assertEqual(
                    as_spark_type(ntp.NDArray[numpy_or_python_type]), ArrayType(spark_type)
                )
                self.assertEqual(
                    pandas_on_spark_type(ntp.NDArray[numpy_or_python_type]),
                    (np.dtype("object"), ArrayType(spark_type)),
                )

        with self.assertRaisesRegex(TypeError, "Type uint64 was not understood."):
            as_spark_type(np.dtype("uint64"))

        with self.assertRaisesRegex(TypeError, "Type object was not understood."):
            as_spark_type(np.dtype("object"))

        with self.assertRaisesRegex(TypeError, "Type uint64 was not understood."):
            pandas_on_spark_type(np.dtype("uint64"))

        with self.assertRaisesRegex(TypeError, "Type object was not understood."):
            pandas_on_spark_type(np.dtype("object"))

    @unittest.skipIf(not extension_dtypes_available, "The pandas extension types are not available")
    def test_as_spark_type_extension_dtypes(self):
        from pandas import Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype

        type_mapper = {
            Int8Dtype(): ByteType(),
            Int16Dtype(): ShortType(),
            Int32Dtype(): IntegerType(),
            Int64Dtype(): LongType(),
        }

        for extension_dtype, spark_type in type_mapper.items():
            self.assertEqual(as_spark_type(extension_dtype), spark_type)
            self.assertEqual(pandas_on_spark_type(extension_dtype), (extension_dtype, spark_type))

    @unittest.skipIf(
        not extension_object_dtypes_available, "The pandas extension object types are not available"
    )
    def test_as_spark_type_extension_object_dtypes(self):
        from pandas import BooleanDtype, StringDtype

        type_mapper = {
            BooleanDtype(): BooleanType(),
            StringDtype(): StringType(),
        }

        for extension_dtype, spark_type in type_mapper.items():
            self.assertEqual(as_spark_type(extension_dtype), spark_type)
            self.assertEqual(pandas_on_spark_type(extension_dtype), (extension_dtype, spark_type))

    @unittest.skipIf(
        not extension_float_dtypes_available, "The pandas extension float types are not available"
    )
    def test_as_spark_type_extension_float_dtypes(self):
        from pandas import Float32Dtype, Float64Dtype

        type_mapper = {
            Float32Dtype(): FloatType(),
            Float64Dtype(): DoubleType(),
        }

        for extension_dtype, spark_type in type_mapper.items():
            self.assertEqual(as_spark_type(extension_dtype), spark_type)
            self.assertEqual(pandas_on_spark_type(extension_dtype), (extension_dtype, spark_type))


class TypeHintTests(TypeHintTestsMixin, unittest.TestCase):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.test_typedef import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

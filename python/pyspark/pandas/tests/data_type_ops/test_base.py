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

import unittest

from pandas.api.types import CategoricalDtype
from pandas.api.extensions import ExtensionDtype

from pyspark.pandas.data_type_ops.base import DataTypeOps
from pyspark.pandas.data_type_ops.binary_ops import BinaryOps
from pyspark.pandas.data_type_ops.boolean_ops import BooleanOps, BooleanExtensionOps
from pyspark.pandas.data_type_ops.categorical_ops import CategoricalOps
from pyspark.pandas.data_type_ops.complex_ops import ArrayOps, MapOps, StructOps
from pyspark.pandas.data_type_ops.date_ops import DateOps
from pyspark.pandas.data_type_ops.datetime_ops import DatetimeOps, DatetimeNTZOps
from pyspark.pandas.data_type_ops.null_ops import NullOps
from pyspark.pandas.data_type_ops.num_ops import IntegralOps, FractionalOps, DecimalOps
from pyspark.pandas.data_type_ops.string_ops import StringOps
from pyspark.pandas.data_type_ops.timedelta_ops import TimedeltaOps
from pyspark.pandas.data_type_ops.udt_ops import UDTOps
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    FractionalType,
    IntegralType,
    MapType,
    NullType,
    StringType,
    StructType,
    TimestampType,
    TimestampNTZType,
    UserDefinedType,
)


class BaseTestsMixin:
    def test_data_type_ops(self):
        _mock_spark_type = DataType()
        _mock_dtype = ExtensionDtype()
        _mappings = (
            (CategoricalDtype(), _mock_spark_type, CategoricalOps),
            (_mock_dtype, DecimalType(), DecimalOps),
            (_mock_dtype, FractionalType(), FractionalOps),
            (_mock_dtype, IntegralType(), IntegralOps),
            (_mock_dtype, StringType(), StringOps),
            (_mock_dtype, BooleanType(), BooleanOps),
            (_mock_dtype, TimestampType(), DatetimeOps),
            (_mock_dtype, TimestampNTZType(), DatetimeNTZOps),
            (_mock_dtype, DateType(), DateOps),
            (_mock_dtype, DayTimeIntervalType(), TimedeltaOps),
            (_mock_dtype, BinaryType(), BinaryOps),
            (_mock_dtype, ArrayType(StringType()), ArrayOps),
            (_mock_dtype, MapType(StringType(), IntegralType()), MapOps),
            (_mock_dtype, StructType(), StructOps),
            (_mock_dtype, NullType(), NullOps),
            (_mock_dtype, UserDefinedType(), UDTOps),
        )
        for _dtype, _spark_type, _ops in _mappings:
            self.assertIsInstance(DataTypeOps(_dtype, _spark_type), _ops)

        _unknow_spark_type = _mock_spark_type
        self.assertRaises(TypeError, DataTypeOps, BooleanType(), _unknow_spark_type)

    def test_bool_ext_ops(self):
        from pyspark.pandas.typedef.typehints import extension_object_dtypes_available

        if extension_object_dtypes_available:
            from pandas import BooleanDtype

            self.assertIsInstance(DataTypeOps(BooleanDtype(), BooleanType()), BooleanExtensionOps)
        else:
            self.assertIsInstance(DataTypeOps(ExtensionDtype(), BooleanType()), BooleanOps)


class BaseTests(
    BaseTestsMixin,
    unittest.TestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.data_type_ops.test_base import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

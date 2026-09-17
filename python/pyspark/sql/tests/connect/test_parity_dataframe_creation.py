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

from pyspark.errors import PySparkNotImplementedError
from pyspark.sql.tests.test_dataframe_creation import DataFrameCreationTestsMixin
from pyspark.sql.types import ArrayType, CharType, StructField, StructType, UserDefinedType
from pyspark.testing.connectutils import ReusedConnectTestCase


class DataFrameCreationParityTests(
    DataFrameCreationTestsMixin,
    ReusedConnectTestCase,
):
    def test_char_varchar_explicit_schema_is_unsupported(self):
        class CharStorageUDT(UserDefinedType):
            @classmethod
            def sqlType(cls):
                return CharType(3)

            @classmethod
            def module(cls):
                return __name__

            @classmethod
            def scalaUDT(cls):
                return ""

            def serialize(self, obj):
                return obj

            def deserialize(self, datum):
                return datum

        schemas = [
            StructType([StructField("value", ArrayType(CharType(3)))]),
            StructType([StructField("value", CharStorageUDT())]),
        ]
        for schema in schemas:
            with self.subTest(schema=schema):
                with self.assertRaisesRegex(
                    PySparkNotImplementedError,
                    "CHAR/VARCHAR in Spark Connect createDataFrame schema",
                ):
                    self.spark.createDataFrame([(["a"],)], schema)


if __name__ == "__main__":
    from pyspark.testing import main

    main()

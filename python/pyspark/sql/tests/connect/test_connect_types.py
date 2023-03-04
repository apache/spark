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

from pyspark.errors import ParseException
from pyspark.sql.connect.types import DDLDataTypeParser, DDLSchemaParser, parse_data_type
from pyspark.sql.types import (
    ArrayType,
    ByteType,
    CharType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructType,
    VarcharType,
)


class DDLParserTests(unittest.TestCase):
    def test_from_datatype(self):
        for data_type_string, expected in [
            ("int ", IntegerType()),
            ("INT ", IntegerType()),
            ("decimal(  16 , 8   ) ", DecimalType(16, 8)),
            ("CHAR( 50 )", CharType(50)),
            ("VARCHAR( 50 )", VarcharType(50)),
            ("array< short>", ArrayType(ShortType())),
            (" map<string , string > ", MapType(StringType(), StringType())),
            (
                "  struct<   a  : byte, b  :decimal(  16 , 8   )  >   ",
                StructType().add("a", ByteType()).add("b", DecimalType(16, 8)),
            ),
            (
                "struct<a:byte  not  nUlL comMent 'aa',b:long  cOMment 'aa\\'bb',c:int not null >",
                StructType()
                .add("a", ByteType(), nullable=False, metadata={"comment": "aa"})
                .add("b", LongType(), metadata={"comment": "aa'bb"})
                .add("c", IntegerType(), nullable=False),
            ),
        ]:
            with self.subTest(data_type_string):
                self.assertEqual(DDLDataTypeParser(data_type_string).from_ddl_datatype(), expected)
            data_type_string = f"struct<a: {data_type_string}>"
            with self.subTest(data_type_string):
                self.assertEqual(
                    DDLDataTypeParser(data_type_string).from_ddl_datatype(),
                    StructType().add("a", expected),
                )

    def test_from_ddl_schema(self):
        for data_type_string, expected in [
            ("a int ", StructType().add("a", IntegerType())),
            ("a INT ", StructType().add("a", IntegerType())),
            (
                "a byte, b decimal(  16 , 8   ) ",
                StructType().add("a", ByteType()).add("b", DecimalType(16, 8)),
            ),
            ("a DOUBLE, b STRING", StructType().add("a", DoubleType()).add("b", StringType())),
            ("a DOUBLE, b CHAR( 50 )", StructType().add("a", DoubleType()).add("b", CharType(50))),
            (
                "a DOUBLE, b VARCHAR( 50 )",
                StructType().add("a", DoubleType()).add("b", VarcharType(50)),
            ),
            ("a array< short>", StructType().add("a", ArrayType(ShortType()))),
            (
                "a map<string , string > ",
                StructType().add("a", MapType(StringType(), StringType())),
            ),
            (
                " ` `` **s` struct<   a  : byte, b  :decimal(  16 , 8   )  >   ",
                StructType().add(
                    " ` **s", StructType().add("a", ByteType()).add("b", DecimalType(16, 8))
                ),
            ),
        ]:
            with self.subTest(data_type_string):
                self.assertEqual(DDLSchemaParser(data_type_string).from_ddl_schema(), expected)

    def test_parse_data_type(self):
        for data_type_string, expected in [
            ("int ", IntegerType()),
            ("INT ", IntegerType()),
            (
                "a: byte, b: decimal(  16 , 8   ) ",
                StructType().add("a", ByteType()).add("b", DecimalType(16, 8)),
            ),
            ("a DOUBLE, b STRING", StructType().add("a", DoubleType()).add("b", StringType())),
            ("a DOUBLE, b CHAR( 50 )", StructType().add("a", DoubleType()).add("b", CharType(50))),
            (
                "a DOUBLE, b VARCHAR( 50 )",
                StructType().add("a", DoubleType()).add("b", VarcharType(50)),
            ),
            ("a: array< short>", StructType().add("a", ArrayType(ShortType()))),
            (" map<string , string > ", MapType(StringType(), StringType())),
        ]:
            with self.subTest(data_type_string):
                self.assertEqual(parse_data_type(data_type_string), expected)

        for data_type_string in ["blabla", "a: int,", "array<int", "map<int, boolean>>"]:
            with self.subTest(data_type_string):
                self.assertRaises(ParseException, lambda: parse_data_type(data_type_string))


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_types import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

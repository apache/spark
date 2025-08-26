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

from pyspark.sql.conversion import ArrowTableToRowsConversion, LocalDataToArrowConversion
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    MapType,
    Row,
    StringType,
    StructType,
)
from pyspark.testing.objects import ExamplePoint, ExamplePointUDT
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ConversionTests(unittest.TestCase):
    def test_conversion(self):
        data = [
            (
                i if i % 2 == 0 else None,
                str(i),
                i,
                str(i).encode(),
                [j if j % 2 == 0 else None for j in range(i)],
                list(range(i)),
                [str(j).encode() for j in range(i)],
                {str(j): j if j % 2 == 0 else None for j in range(i)},
                {str(j): j for j in range(i)},
                {str(j): str(j).encode() for j in range(i)},
                (i if i % 2 == 0 else None, str(i), i, str(i).encode()),
                {"i": i if i % 2 == 0 else None, "s": str(i), "ii": i, "b": str(i).encode()},
                ExamplePoint(float(i), float(i)),
            )
            for i in range(5)
        ]
        schema = (
            StructType()
            .add("i", IntegerType())
            .add("s", StringType())
            .add("ii", IntegerType(), nullable=False)
            .add("b", BinaryType())
            .add("arr_i", ArrayType(IntegerType()))
            .add("arr_ii", ArrayType(IntegerType(), containsNull=False))
            .add("arr_b", ArrayType(BinaryType()))
            .add("map_i", MapType(StringType(), IntegerType()))
            .add("map_ii", MapType(StringType(), IntegerType(), valueContainsNull=False))
            .add("map_b", MapType(StringType(), BinaryType()))
            .add(
                "struct_t",
                StructType()
                .add("i", IntegerType())
                .add("s", StringType())
                .add("ii", IntegerType(), nullable=False)
                .add("b", BinaryType()),
            )
            .add(
                "struct_d",
                StructType()
                .add("i", IntegerType())
                .add("s", StringType())
                .add("ii", IntegerType(), nullable=False)
                .add("b", BinaryType()),
            )
            .add("udt", ExamplePointUDT())
        )

        tbl = LocalDataToArrowConversion.convert(data, schema, use_large_var_types=False)
        actual = ArrowTableToRowsConversion.convert(tbl, schema)

        for a, e in zip(
            actual,
            [
                Row(
                    i=i if i % 2 == 0 else None,
                    s=str(i),
                    ii=i,
                    b=str(i).encode(),
                    arr_i=[j if j % 2 == 0 else None for j in range(i)],
                    arr_ii=list(range(i)),
                    arr_b=[str(j).encode() for j in range(i)],
                    map_i={str(j): j if j % 2 == 0 else None for j in range(i)},
                    map_ii={str(j): j for j in range(i)},
                    map_b={str(j): str(j).encode() for j in range(i)},
                    struct_t=Row(i=i if i % 2 == 0 else None, s=str(i), ii=i, b=str(i).encode()),
                    struct_d=Row(i=i if i % 2 == 0 else None, s=str(i), ii=i, b=str(i).encode()),
                    udt=ExamplePoint(float(i), float(i)),
                )
                for i in range(5)
            ],
        ):
            with self.subTest(expected=e):
                self.assertEqual(a, e)


if __name__ == "__main__":
    from pyspark.sql.tests.test_conversion import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

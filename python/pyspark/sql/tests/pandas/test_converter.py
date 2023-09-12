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
from typing import cast

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructType,
    Row,
)
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pandas:
    import pandas as pd
    import numpy as np

    from pandas.testing import assert_series_equal
    from pyspark.sql.pandas.types import _create_converter_from_pandas, _create_converter_to_pandas

if have_pyarrow:
    import pyarrow as pa  # noqa: F401


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    cast(str, pandas_requirement_message or pyarrow_requirement_message),
)
class ConverterTests(unittest.TestCase):
    def test_converter_to_pandas_array(self):
        # _element_conv is None
        conv = _create_converter_to_pandas(ArrayType(IntegerType()))
        pser = pd.Series([[1, 2, 3, None], np.array([4, 5, None]), None])
        self.assertIs(conv(pser), pser)

        # _element_conv is not None
        conv = _create_converter_to_pandas(
            ArrayType(StructType().add("a", IntegerType())), struct_in_pandas="dict"
        )
        pser = pd.Series([[Row(a=1), Row(a=2), None], np.array([{"a": 3}, None]), None])
        assert_series_equal(
            conv(pser), pd.Series([[{"a": 1}, {"a": 2}, None], np.array([{"a": 3}, None]), None])
        )

        # ndarray_as_list=True

        # _element_conv is None
        conv = _create_converter_to_pandas(ArrayType(IntegerType()), ndarray_as_list=True)
        pser = pd.Series([[1, 2, 3, None], np.array([4, 5, None]), None])
        assert_series_equal(conv(pser), pd.Series([[1, 2, 3, None], [4, 5, None], None]))

        # _element_conv is not None
        conv = _create_converter_to_pandas(
            ArrayType(StructType().add("a", IntegerType())),
            struct_in_pandas="dict",
            ndarray_as_list=True,
        )
        pser = pd.Series([[Row(a=1), Row(a=2), None], np.array([{"a": 3}, None]), None])
        assert_series_equal(
            conv(pser), pd.Series([[{"a": 1}, {"a": 2}, None], [{"a": 3}, None], None])
        )

    def test_converter_from_pandas_array(self):
        # _element_conv is None
        conv = _create_converter_from_pandas(ArrayType(IntegerType()))
        pser = pd.Series([[1, 2, 3, None], np.array([4, 5, None]), None])
        assert_series_equal(
            conv(pser),
            pd.Series([[1, 2, 3, None], [4, 5, None], None]),
        )

        # _element_conv is not None
        conv = _create_converter_from_pandas(ArrayType(StructType().add("a", IntegerType())))
        pser = pd.Series(
            [[{"a": 1}, {"a": 2}, {"a": 3}, None], np.array([{"a": 4}, {"a": 5}, None]), None]
        )
        assert_series_equal(
            conv(pser),
            pd.Series([[{"a": 1}, {"a": 2}, {"a": 3}, None], [{"a": 4}, {"a": 5}, None], None]),
        )

        # ignore_unexpected_complex_type_values=True

        # _element_conv is None
        conv = _create_converter_from_pandas(
            ArrayType(IntegerType()), ignore_unexpected_complex_type_values=True
        )
        pser = pd.Series([[1, 2, 3, None], np.array([4, 5, None]), None, 100])
        assert_series_equal(conv(pser), pd.Series([[1, 2, 3, None], [4, 5, None], None, 100]))

        # _element_conv is not None
        conv = _create_converter_from_pandas(
            ArrayType(StructType().add("a", IntegerType())),
            ignore_unexpected_complex_type_values=True,
        )
        pser = pd.Series(
            [[{"a": 1}, {"a": 2}, {"a": 3}, None], np.array([{"a": 4}, {"a": 5}, None]), None, 100]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [[{"a": 1}, {"a": 2}, {"a": 3}, None], [{"a": 4}, {"a": 5}, None], None, 100]
            ),
        )

    def test_converter_to_pandas_map(self):
        # _key_conv is None and _value_conv is None
        conv = _create_converter_to_pandas(MapType(StringType(), IntegerType()))
        pser = pd.Series(
            [[("x", 1), ("y", 2), ("z", None), (None, 3)], {"x": 4, "y": None, None: 5}, None]
        )
        assert_series_equal(
            conv(pser),
            pd.Series([{"x": 1, "y": 2, "z": None, None: 3}, {"x": 4, "y": None, None: 5}, None]),
        )

        # _key_conv is None and _value_conv is not None
        conv = _create_converter_to_pandas(
            MapType(StringType(), StructType().add("a", IntegerType())), struct_in_pandas="row"
        )
        pser = pd.Series(
            [
                [("x", Row(a=1)), ("y", Row(a=2)), ("z", None), (None, Row(a=3))],
                {"x": Row(a=4), "y": None, None: Row(a=5)},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    {"x": Row(a=1), "y": Row(a=2), "z": None, None: Row(a=3)},
                    {"x": Row(a=4), "y": None, None: Row(a=5)},
                    None,
                ]
            ),
        )

        # _key_conv is not None and _value_conv is None
        conv = _create_converter_to_pandas(
            MapType(StructType().add("a", StringType()), IntegerType()), struct_in_pandas="row"
        )
        pser = pd.Series(
            [
                [(Row(a="x"), 1), (Row(a="y"), 2), (Row(a="z"), None), (None, 3)],
                {Row(a="x"): 4, Row(a="y"): None, None: 5},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    {Row(a="x"): 1, Row(a="y"): 2, Row(a="z"): None, None: 3},
                    {Row(a="x"): 4, Row(a="y"): None, None: 5},
                    None,
                ]
            ),
        )

    def test_converter_from_pandas_map(self):
        # _key_conv is None and _value_conv is None
        conv = _create_converter_from_pandas(MapType(StringType(), IntegerType()))
        pser = pd.Series([{"x": 1, "y": 2, "z": None, None: 3}, {"x": 4, "y": None, None: 5}, None])
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    [("x", 1), ("y", 2), ("z", None), (None, 3)],
                    [("x", 4), ("y", None), (None, 5)],
                    None,
                ]
            ),
        )

        # _key_conv is None and _value_conv is not None
        conv = _create_converter_from_pandas(
            MapType(StringType(), StructType().add("a", IntegerType()))
        )
        pser = pd.Series(
            [
                {"x": Row(a=1), "y": Row(a=2), "z": None, None: Row(a=3)},
                {"x": Row(a=4), "y": None, None: Row(a=5)},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    [("x", {"a": 1}), ("y", {"a": 2}), ("z", None), (None, {"a": 3})],
                    [("x", {"a": 4}), ("y", None), (None, {"a": 5})],
                    None,
                ]
            ),
        )

        # _key_conv is None and _value_conv is not None
        conv = _create_converter_from_pandas(
            MapType(StructType().add("a", StringType()), IntegerType())
        )
        pser = pd.Series(
            [
                {Row(a="x"): 1, Row(a="y"): 2, Row(a="z"): None, None: 3},
                {Row(a="x"): 4, Row(a="y"): None, None: 5},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    [({"a": "x"}, 1), ({"a": "y"}, 2), ({"a": "z"}, None), (None, 3)],
                    [({"a": "x"}, 4), ({"a": "y"}, None), (None, 5)],
                    None,
                ]
            ),
        )

        # ignore_unexpected_complex_type_values=True

        # _key_conv is None and _value_conv is None
        conv = _create_converter_from_pandas(
            MapType(StringType(), IntegerType()), ignore_unexpected_complex_type_values=True
        )
        pser = pd.Series(
            [{"x": 1, "y": 2, "z": None, None: 3}, {"x": 4, "y": None, None: 5}, None, 100]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    [("x", 1), ("y", 2), ("z", None), (None, 3)],
                    [("x", 4), ("y", None), (None, 5)],
                    None,
                    100,
                ]
            ),
        )

        # _key_conv is None and _value_conv is not None
        conv = _create_converter_from_pandas(
            MapType(StringType(), StructType().add("a", IntegerType())),
            ignore_unexpected_complex_type_values=True,
        )
        pser = pd.Series(
            [
                {"x": Row(a=1), "y": Row(a=2), "z": None, None: Row(a=3)},
                {"x": Row(a=4), "y": None, None: Row(a=5)},
                None,
                100,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    [("x", {"a": 1}), ("y", {"a": 2}), ("z", None), (None, {"a": 3})],
                    [("x", {"a": 4}), ("y", None), (None, {"a": 5})],
                    None,
                    100,
                ]
            ),
        )

        # _key_conv is None and _value_conv is not None
        conv = _create_converter_from_pandas(
            MapType(StructType().add("a", StringType()), IntegerType()),
            ignore_unexpected_complex_type_values=True,
        )
        pser = pd.Series(
            [
                {Row(a="x"): 1, Row(a="y"): 2, Row(a="z"): None, None: 3},
                {Row(a="x"): 4, Row(a="y"): None, None: 5},
                None,
                100,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    [({"a": "x"}, 1), ({"a": "y"}, 2), ({"a": "z"}, None), (None, 3)],
                    [({"a": "x"}, 4), ({"a": "y"}, None), (None, 5)],
                    None,
                    100,
                ]
            ),
        )

    def test_converter_to_pandas_struct(self):
        # struct_in_pandas="row"

        # all the convs are None
        conv = _create_converter_to_pandas(
            StructType().add("x", StringType()).add("y", IntegerType()), struct_in_pandas="row"
        )
        pser = pd.Series(
            [
                Row(x="a", y=1),
                Row(x="b", y=2),
                Row(x="c", y=None),
                Row(x=None, y=3),
                {"x": "d", "y": 4},
                {"x": "e"},
                {"y": 5},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    Row(x="a", y=1),
                    Row(x="b", y=2),
                    Row(x="c", y=None),
                    Row(x=None, y=3),
                    Row(x="d", y=4),
                    Row(x="e", y=None),
                    Row(x=None, y=5),
                    None,
                ]
            ),
        )

        # one of the convs is not None
        conv = _create_converter_to_pandas(
            StructType().add("x", StringType()).add("y", StructType().add("i", IntegerType())),
            struct_in_pandas="row",
        )
        pser = pd.Series(
            [
                Row(x="a", y=Row(i=1)),
                Row(x="b", y={"i": 2}),
                Row(x="c", y=None),
                Row(x=None, y=Row(i=3)),
                {"x": "d", "y": Row(i=4)},
                {"x": "e"},
                {"y": {"i": 5}},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    Row(x="a", y=Row(i=1)),
                    Row(x="b", y=Row(i=2)),
                    Row(x="c", y=None),
                    Row(x=None, y=Row(i=3)),
                    Row(x="d", y=Row(i=4)),
                    Row(x="e", y=None),
                    Row(x=None, y=Row(i=5)),
                    None,
                ]
            ),
        )

        # struct_in_pandas="dict"

        # all the convs are None
        conv = _create_converter_to_pandas(
            StructType().add("x", StringType()).add("y", IntegerType()), struct_in_pandas="dict"
        )
        pser = pd.Series(
            [
                Row(x="a", y=1),
                Row(x="b", y=2),
                Row(x="c", y=None),
                Row(x=None, y=3),
                {"x": "d", "y": 4},
                {"x": "e"},
                {"y": 5},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    {"x": "a", "y": 1},
                    {"x": "b", "y": 2},
                    {"x": "c", "y": None},
                    {"x": None, "y": 3},
                    {"x": "d", "y": 4},
                    {"x": "e", "y": None},
                    {"x": None, "y": 5},
                    None,
                ]
            ),
        )

        # one of the convs is not None
        conv = _create_converter_to_pandas(
            StructType().add("x", StringType()).add("y", StructType().add("i", IntegerType())),
            struct_in_pandas="dict",
        )
        pser = pd.Series(
            [
                Row(x="a", y=Row(i=1)),
                Row(x="b", y={"i": 2}),
                Row(x="c", y=None),
                Row(x=None, y=Row(i=3)),
                {"x": "d", "y": Row(i=4)},
                {"x": "e"},
                {"y": {"i": 5}},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    {"x": "a", "y": {"i": 1}},
                    {"x": "b", "y": {"i": 2}},
                    {"x": "c", "y": None},
                    {"x": None, "y": {"i": 3}},
                    {"x": "d", "y": {"i": 4}},
                    {"x": "e", "y": None},
                    {"x": None, "y": {"i": 5}},
                    None,
                ]
            ),
        )

    def test_converter_from_pandas_struct(self):
        # all the convs are None
        conv = _create_converter_from_pandas(
            StructType().add("x", StringType()).add("y", IntegerType())
        )
        pser = pd.Series(
            [
                Row(x="a", y=1),
                Row(x="b", y=2),
                Row(x="c", y=None),
                Row(x=None, y=3),
                {"x": "d", "y": 4},
                {"x": "e"},
                {"y": 5},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    {"x": "a", "y": 1},
                    {"x": "b", "y": 2},
                    {"x": "c", "y": None},
                    {"x": None, "y": 3},
                    {"x": "d", "y": 4},
                    {"x": "e", "y": None},
                    {"x": None, "y": 5},
                    None,
                ]
            ),
        )

        # one of the convs is not None
        conv = _create_converter_from_pandas(
            StructType().add("x", StringType()).add("y", StructType().add("i", IntegerType()))
        )
        pser = pd.Series(
            [
                Row(x="a", y=Row(i=1)),
                Row(x="b", y={"i": 2}),
                Row(x="c", y=None),
                Row(x=None, y=Row(i=3)),
                {"x": "d", "y": Row(i=4)},
                {"x": "e"},
                {"y": {"i": 5}},
                None,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    {"x": "a", "y": {"i": 1}},
                    {"x": "b", "y": {"i": 2}},
                    {"x": "c", "y": None},
                    {"x": None, "y": {"i": 3}},
                    {"x": "d", "y": {"i": 4}},
                    {"x": "e", "y": None},
                    {"x": None, "y": {"i": 5}},
                    None,
                ]
            ),
        )

        # ignore_unexpected_complex_type_values=True

        # all the convs are None
        conv = _create_converter_from_pandas(
            StructType().add("x", StringType()).add("y", IntegerType()),
            ignore_unexpected_complex_type_values=True,
        )
        pser = pd.Series(
            [
                Row(x="a", y=1),
                Row(x="b", y=2),
                Row(x="c", y=None),
                Row(x=None, y=3),
                {"x": "d", "y": 4},
                {"x": "e"},
                {"y": 5},
                None,
                100,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    {"x": "a", "y": 1},
                    {"x": "b", "y": 2},
                    {"x": "c", "y": None},
                    {"x": None, "y": 3},
                    {"x": "d", "y": 4},
                    {"x": "e", "y": None},
                    {"x": None, "y": 5},
                    None,
                    100,
                ]
            ),
        )

        # one of the convs is not None
        conv = _create_converter_from_pandas(
            StructType().add("x", StringType()).add("y", StructType().add("i", IntegerType())),
            ignore_unexpected_complex_type_values=True,
        )
        pser = pd.Series(
            [
                Row(x="a", y=Row(i=1)),
                Row(x="b", y={"i": 2}),
                Row(x="c", y=None),
                Row(x=None, y=Row(i=3)),
                {"x": "d", "y": Row(i=4)},
                {"x": "e"},
                {"y": {"i": 5}},
                None,
                100,
            ]
        )
        assert_series_equal(
            conv(pser),
            pd.Series(
                [
                    {"x": "a", "y": {"i": 1}},
                    {"x": "b", "y": {"i": 2}},
                    {"x": "c", "y": None},
                    {"x": None, "y": {"i": 3}},
                    {"x": "d", "y": {"i": 4}},
                    {"x": "e", "y": None},
                    {"x": None, "y": {"i": 5}},
                    None,
                    100,
                ]
            ),
        )


if __name__ == "__main__":
    from pyspark.sql.tests.pandas.test_converter import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

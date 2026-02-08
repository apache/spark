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
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructType,
    TimestampType,
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
    import pyarrow as pa
    from pyspark.sql.conversion import ArrowArrayToPandasConversion


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
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


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class ArrowArrayToPandasConversionTests(unittest.TestCase):
    """Tests for ArrowArrayToPandasConversion.convert_numpy with complex types."""

    def test_simple_map(self):
        """Test simple map conversion."""
        arrow_type = pa.map_(pa.string(), pa.int64())
        arr = pa.array([[("a", 1), ("b", 2)], [("c", 3)]], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr,
            MapType(StringType(), IntegerType()),
        )

        self.assertEqual(result.iloc[0], {"a": 1, "b": 2})
        self.assertEqual(result.iloc[1], {"c": 3})

    def test_simple_array(self):
        """Test simple array conversion."""
        arrow_type = pa.list_(pa.int64())
        arr = pa.array([[1, 2, 3], [4, 5]], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr,
            ArrayType(IntegerType()),
        )

        self.assertIsInstance(result.iloc[0], np.ndarray)
        self.assertEqual(list(result.iloc[0]), [1, 2, 3])

    def test_simple_struct(self):
        """Test simple struct conversion."""
        arrow_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        arr = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr,
            StructType().add("x", IntegerType()).add("y", StringType()),
        )

        self.assertIsInstance(result.iloc[0], dict)
        self.assertEqual(result.iloc[0], {"x": 1, "y": "a"})

    def test_nested_struct_in_map(self):
        """Test map with nested struct values."""
        arrow_type = pa.map_(pa.string(), pa.struct([("x", pa.int64()), ("y", pa.string())]))
        arr = pa.array([[("key1", {"x": 1, "y": "a"})]], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr,
            MapType(StringType(), StructType().add("x", IntegerType()).add("y", StringType())),
        )

        self.assertIsInstance(result.iloc[0], dict)
        self.assertIsInstance(result.iloc[0]["key1"], dict)
        self.assertEqual(result.iloc[0]["key1"]["x"], 1)

    def test_map_with_array_values(self):
        """Test map with array values."""
        arrow_type = pa.map_(pa.string(), pa.list_(pa.int64()))
        arr = pa.array([[("nums", [1, 2, 3]), ("more", [4, 5])]], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr,
            MapType(StringType(), ArrayType(IntegerType())),
        )

        self.assertIsInstance(result.iloc[0], dict)
        self.assertEqual(list(result.iloc[0]["nums"]), [1, 2, 3])

    def test_struct_with_array_and_map_fields(self):
        """Test struct containing both array and map fields."""
        arrow_type = pa.struct(
            [
                ("name", pa.string()),
                ("scores", pa.list_(pa.int64())),
                ("metadata", pa.map_(pa.string(), pa.string())),
            ]
        )
        arr = pa.array(
            [{"name": "test", "scores": [90, 85, 88], "metadata": [("key", "value")]}],
            type=arrow_type,
        )

        spark_type = (
            StructType()
            .add("name", StringType())
            .add("scores", ArrayType(IntegerType()))
            .add("metadata", MapType(StringType(), StringType()))
        )

        result = ArrowArrayToPandasConversion.convert_numpy(arr, spark_type)

        self.assertEqual(result.iloc[0]["name"], "test")
        self.assertEqual(list(result.iloc[0]["scores"]), [90, 85, 88])
        self.assertEqual(result.iloc[0]["metadata"], {"key": "value"})

    def test_empty_array(self):
        """Test empty array."""
        arrow_type = pa.list_(pa.int64())
        arr = pa.array([[], [1, 2], []], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(arr, ArrayType(IntegerType()))

        self.assertEqual(len(result.iloc[0]), 0)
        self.assertEqual(list(result.iloc[1]), [1, 2])

    def test_empty_map(self):
        """Test empty map."""
        arrow_type = pa.map_(pa.string(), pa.int64())
        arr = pa.array([[], [("a", 1)], []], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, MapType(StringType(), IntegerType())
        )

        self.assertEqual(result.iloc[0], {})
        self.assertEqual(result.iloc[1], {"a": 1})

    def test_null_in_array(self):
        """Test array containing null values."""
        arrow_type = pa.list_(pa.int64())
        arr = pa.array([[1, None, 3], None, [4, 5]], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(arr, ArrayType(IntegerType()))

        self.assertEqual(len(result), 3)
        self.assertTrue(np.isnan(result.iloc[0][1]))
        self.assertIsNone(result.iloc[1])

    def test_null_in_map(self):
        """Test map containing null values."""
        arrow_type = pa.map_(pa.string(), pa.int64())
        arr = pa.array([[("a", 1), ("b", None)], None], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, MapType(StringType(), IntegerType())
        )

        self.assertEqual(result.iloc[0]["a"], 1)
        self.assertIsNone(result.iloc[0]["b"])
        self.assertIsNone(result.iloc[1])

    def test_null_in_struct(self):
        """Test struct containing null values."""
        arrow_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        arr = pa.array([{"x": 1, "y": None}, None], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr, StructType().add("x", IntegerType()).add("y", StringType())
        )

        self.assertEqual(result.iloc[0]["x"], 1)
        self.assertIsNone(result.iloc[0]["y"])
        self.assertIsNone(result.iloc[1])

    def test_map_with_timestamp_values(self):
        """Test that nested timestamps are localized by ArrowTimestampConversion."""
        import datetime

        # 2023-01-15 10:30:00 UTC
        ts = datetime.datetime(2023, 1, 15, 10, 30, 0)
        arrow_type = pa.map_(pa.string(), pa.timestamp("us", tz="UTC"))
        arr = pa.array([[("key1", ts)]], type=arrow_type)

        result = ArrowArrayToPandasConversion.convert_numpy(
            arr,
            MapType(StringType(), TimestampType()),
        )

        # Verify the result is a dict and timestamp is timezone-naive (localized)
        self.assertIsInstance(result.iloc[0], dict)
        converted_ts = result.iloc[0]["key1"]
        self.assertIsInstance(converted_ts, datetime.datetime)
        self.assertIsNone(converted_ts.tzinfo)

    def test_ndarray_as_list(self):
        """Test ndarray_as_list=True converts arrays to lists via convert()."""
        arrow_type = pa.list_(pa.int64())
        arr = pa.array([[1, 2, 3], [4, 5]], type=arrow_type)

        # ndarray_as_list=True routes to convert_legacy via convert()
        result = ArrowArrayToPandasConversion.convert(
            arr,
            ArrayType(IntegerType()),
            ndarray_as_list=True,
        )

        self.assertIsInstance(result.iloc[0], list)
        self.assertEqual(result.iloc[0], [1, 2, 3])

    def test_struct_in_pandas_row(self):
        """Test struct_in_pandas='row' converts structs to Row objects via convert()."""
        arrow_type = pa.struct([("name", pa.string()), ("age", pa.int64())])
        arr = pa.array([{"name": "Alice", "age": 30}], type=arrow_type)

        # struct_in_pandas="row" routes to convert_legacy via convert()
        result = ArrowArrayToPandasConversion.convert(
            arr,
            StructType().add("name", StringType()).add("age", IntegerType()),
            struct_in_pandas="row",
        )

        self.assertIsInstance(result.iloc[0], Row)
        self.assertEqual(result.iloc[0].name, "Alice")
        self.assertEqual(result.iloc[0].age, 30)


if __name__ == "__main__":
    from pyspark.testing import main

    main()

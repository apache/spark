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
import os
import sys
import tempfile
import unittest
from unittest import mock

from pyspark import Broadcast, SparkConf, SparkContext
from pyspark.core.broadcast import (
    _ARROW_ARRAY_SERIALIZATION,
    _ARROW_CHUNKED_ARRAY_SERIALIZATION,
    _ARROW_RECORD_BATCH_SERIALIZATION,
    _ARROW_TABLE_SERIALIZATION,
    _BROADCAST_FORMAT_MAGIC,
    _CUSTOM_ARROW_SERIALIZATION,
    _PICKLE_SERIALIZATION,
)
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message


class ExampleArrowBroadcastValue:
    def __init__(self, values):
        self.values = values

    def __to_arrow__(self):
        import pyarrow as pa

        return pa.array(self.values)

    @classmethod
    def __from_arrow__(cls, value):
        return cls(value.to_pylist())


class ExampleArrowStreamBroadcastValue:
    def __init__(self, values):
        self.values = values

    def __arrow_c_stream__(self, requested_schema=None):
        import pyarrow as pa

        return pa.table({"value": self.values}).__arrow_c_stream__(requested_schema)

    @classmethod
    def __from_arrow__(cls, value):
        return cls(value.column("value").to_pylist())


class ExampleArrowBroadcastWithoutReconstructor:
    def __init__(self, values):
        self.values = values

    def __to_arrow__(self):
        import pyarrow as pa

        return pa.array(self.values)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowBroadcastTest(unittest.TestCase):
    def tearDown(self):
        if getattr(self, "sc", None) is not None:
            self.sc.stop()
            self.sc = None

    def _test_arrow_broadcast(self, *extra_confs):
        import pyarrow as pa

        conf = SparkConf()
        for key, value in extra_confs:
            conf.set(key, value)
        conf.setMaster("local[2]")
        self.sc = SparkContext(conf=conf)

        values = [
            pa.table({"i": [1, 2], "s": ["a", "b"]}),
            pa.record_batch([[3, 4]], names=["i"]),
            pa.array([], type=pa.int64()),
            pa.chunked_array([[5, 6], [7]], type=pa.int64()),
        ]
        broadcasts = [self.sc.broadcast(value, useArrow=True) for value in values]
        custom_broadcasts = [
            self.sc.broadcast(ExampleArrowBroadcastValue([8, 9]), useArrow=True),
            self.sc.broadcast(ExampleArrowStreamBroadcastValue([10, 11]), useArrow=True),
        ]

        for broadcast, expected in zip(broadcasts, values):
            self.assertIs(type(broadcast.value), type(expected))
            self.assertTrue(broadcast.value.equals(expected))
        self.assertEqual([b.value.values for b in custom_broadcasts], [[8, 9], [10, 11]])

        def read_broadcasts(_):
            table, batch, array, chunked_array = [broadcast.value for broadcast in broadcasts]
            return (
                table.to_pydict(),
                batch.to_pydict(),
                array.to_pylist(),
                chunked_array.to_pylist(),
                [broadcast.value.values for broadcast in custom_broadcasts],
            )

        expected = (
            {"i": [1, 2], "s": ["a", "b"]},
            {"i": [3, 4]},
            [],
            [5, 6, 7],
            [[8, 9], [10, 11]],
        )
        results = self.sc.parallelize(range(2), 2).map(read_broadcasts).collect()
        self.assertEqual(results, [expected, expected])

    def test_arrow_broadcast(self):
        self._test_arrow_broadcast()

    def test_arrow_broadcast_with_encryption(self):
        self._test_arrow_broadcast(("spark.io.encryption.enabled", "true"))

    def test_arrow_broadcast_is_opt_in(self):
        import pyarrow as pa

        self.sc = SparkContext(master="local[1]")
        value = pa.array([1, 2, 3])
        broadcasts = [
            self.sc.broadcast(value),
            self.sc.broadcast(value, useArrow=False),
        ]
        for broadcast in broadcasts:
            with open(broadcast._path, "rb") as serialized:
                self.assertEqual(
                    serialized.read(len(_BROADCAST_FORMAT_MAGIC)),
                    _BROADCAST_FORMAT_MAGIC,
                )
                self.assertEqual(serialized.read(1), _PICKLE_SERIALIZATION)


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowBroadcastSerializationTest(unittest.TestCase):
    def test_arrow_serialization_preserves_type_and_value(self):
        import pyarrow as pa

        values_and_serializations = [
            (pa.table({"i": [1, 2]}), _ARROW_TABLE_SERIALIZATION),
            (pa.record_batch([[1, 2]], names=["i"]), _ARROW_RECORD_BATCH_SERIALIZATION),
            (pa.array([], type=pa.int64()), _ARROW_ARRAY_SERIALIZATION),
            (
                pa.chunked_array([[1, 2], [3]], type=pa.int64()),
                _ARROW_CHUNKED_ARRAY_SERIALIZATION,
            ),
        ]
        broadcast = Broadcast.__new__(Broadcast)

        for value, expected_serialization in values_and_serializations:
            output = tempfile.NamedTemporaryFile(delete=False)
            path = output.name
            try:
                broadcast._dump(value, output, use_arrow=True)
                with open(path, "rb") as serialized:
                    self.assertEqual(
                        serialized.read(len(_BROADCAST_FORMAT_MAGIC)),
                        _BROADCAST_FORMAT_MAGIC,
                    )
                    self.assertEqual(serialized.read(1), expected_serialization)

                actual = broadcast._load_from_path(path)
                self.assertIs(type(actual), type(value))
                self.assertTrue(actual.equals(value))
            finally:
                os.unlink(path)

    def test_custom_arrow_serialization(self):
        values = [
            ExampleArrowBroadcastValue([1, 2]),
            ExampleArrowStreamBroadcastValue([3, 4]),
        ]
        broadcast = Broadcast.__new__(Broadcast)

        for value in values:
            output = tempfile.NamedTemporaryFile(delete=False)
            path = output.name
            try:
                broadcast._dump(value, output, use_arrow=True)
                with open(path, "rb") as serialized:
                    self.assertEqual(
                        serialized.read(len(_BROADCAST_FORMAT_MAGIC)),
                        _BROADCAST_FORMAT_MAGIC,
                    )
                    self.assertEqual(serialized.read(1), _CUSTOM_ARROW_SERIALIZATION)

                actual = broadcast._load_from_path(path)
                self.assertIs(type(actual), type(value))
                self.assertEqual(actual.values, value.values)
            finally:
                os.unlink(path)

    def test_pickle_fallback(self):
        broadcast = Broadcast.__new__(Broadcast)
        values_and_use_arrow = [
            ({"a": [1, 2, 3]}, True),
            (ExampleArrowBroadcastWithoutReconstructor([4, 5]), True),
        ]

        for value, use_arrow in values_and_use_arrow:
            output = tempfile.NamedTemporaryFile(delete=False)
            path = output.name
            try:
                broadcast._dump(value, output, use_arrow=use_arrow)
                with open(path, "rb") as serialized:
                    self.assertEqual(
                        serialized.read(len(_BROADCAST_FORMAT_MAGIC)),
                        _BROADCAST_FORMAT_MAGIC,
                    )
                    self.assertEqual(serialized.read(1), _PICKLE_SERIALIZATION)
                actual = broadcast._load_from_path(path)
                self.assertIs(type(actual), type(value))
                if isinstance(value, dict):
                    self.assertEqual(actual, value)
                else:
                    self.assertEqual(actual.values, value.values)
            finally:
                os.unlink(path)

    def test_pickle_fallback_without_pyarrow(self):
        broadcast = Broadcast.__new__(Broadcast)
        value = ExampleArrowBroadcastValue([1, 2])
        output = tempfile.NamedTemporaryFile(delete=False)
        path = output.name
        try:
            with mock.patch.dict(sys.modules, {"pyarrow": None}):
                broadcast._dump(value, output, use_arrow=True)
            with open(path, "rb") as serialized:
                self.assertEqual(
                    serialized.read(len(_BROADCAST_FORMAT_MAGIC)),
                    _BROADCAST_FORMAT_MAGIC,
                )
                self.assertEqual(serialized.read(1), _PICKLE_SERIALIZATION)
            actual = broadcast._load_from_path(path)
            self.assertIs(type(actual), type(value))
            self.assertEqual(actual.values, value.values)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    from pyspark.testing import main

    main()

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
import pickle
import random
import sys
import time
import tempfile
import unittest
from unittest import mock

from py4j.protocol import Py4JJavaError

from pyspark import SparkConf, SparkContext, Broadcast
from pyspark.core.broadcast import (
    _ARROW_ARRAY_SERIALIZATION,
    _ARROW_CHUNKED_ARRAY_SERIALIZATION,
    _ARROW_RECORD_BATCH_SERIALIZATION,
    _ARROW_TABLE_SERIALIZATION,
    _BROADCAST_FORMAT_MAGIC,
    _CUSTOM_ARROW_SERIALIZATION,
    _PICKLE_SERIALIZATION,
)
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import ChunkedStream
from pyspark.sql import SparkSession, Row
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message


class _ArrowBroadcastValue:
    def __init__(self, values):
        self.values = values

    def __to_arrow__(self):
        import pyarrow as pa

        return pa.array(self.values)

    @classmethod
    def __from_arrow__(cls, value):
        return cls(value.to_pylist())


class _ArrowStreamBroadcastValue:
    def __init__(self, values):
        self.values = values

    def __arrow_c_stream__(self, requested_schema=None):
        import pyarrow as pa

        return pa.table({"value": self.values}).__arrow_c_stream__(requested_schema)

    @classmethod
    def __from_arrow__(cls, value):
        return cls(value.column("value").to_pylist())


class _ArrowBroadcastWithoutReconstructor:
    def __init__(self, values):
        self.values = values

    def __to_arrow__(self):
        import pyarrow as pa

        return pa.array(self.values)


class BroadcastTest(unittest.TestCase):
    def tearDown(self):
        if getattr(self, "sc", None) is not None:
            self.sc.stop()
            self.sc = None

    def _test_encryption_helper(self, vs):
        """
        Creates a broadcast variables for each value in vs, and runs a simple job to make sure the
        value is the same when it's read in the executors.  Also makes sure there are no task
        failures.
        """
        bs = [self.sc.broadcast(value=v) for v in vs]
        exec_values = self.sc.parallelize(range(2)).map(lambda x: [b.value for b in bs]).collect()
        for ev in exec_values:
            self.assertEqual(ev, vs)
        # make sure there are no task failures
        status = self.sc.statusTracker()
        for jid in status.getJobIdsForGroup():
            for sid in status.getJobInfo(jid).stageIds:
                stage_info = status.getStageInfo(sid)
                self.assertEqual(0, stage_info.numFailedTasks)

    def _test_multiple_broadcasts(self, *extra_confs):
        """
        Test broadcast variables make it OK to the executors.  Tests multiple broadcast variables,
        and also multiple jobs.
        """
        conf = SparkConf()
        for key, value in extra_confs:
            conf.set(key, value)
        conf.setMaster("local-cluster[2,1,1024]")
        self.sc = SparkContext(conf=conf)
        self._test_encryption_helper([5])
        self._test_encryption_helper([5, 10, 20])

    def test_broadcast_with_encryption(self):
        self._test_multiple_broadcasts(("spark.io.encryption.enabled", "true"))

    def test_broadcast_no_encryption(self):
        self._test_multiple_broadcasts()

    def _test_broadcast_on_driver(self, *extra_confs):
        conf = SparkConf()
        for key, value in extra_confs:
            conf.set(key, value)
        conf.setMaster("local-cluster[2,1,1024]")
        self.sc = SparkContext(conf=conf)
        bs = self.sc.broadcast(value=5)
        self.assertEqual(5, bs.value)

    def test_broadcast_value_driver_no_encryption(self):
        self._test_broadcast_on_driver()

    def test_broadcast_value_driver_encryption(self):
        self._test_broadcast_on_driver(("spark.io.encryption.enabled", "true"))

    def test_broadcast_value_against_gc(self):
        # Test broadcast value against gc.
        conf = SparkConf()
        conf.setMaster("local[1,1]")
        conf.set("spark.memory.fraction", "0.0001")
        self.sc = SparkContext(conf=conf)
        b = self.sc.broadcast([100])
        try:
            res = self.sc.parallelize([0], 1).map(lambda x: 0 if x == 0 else b.value[0]).collect()
            self.assertEqual([0], res)
            self.sc._jvm.java.lang.System.gc()
            time.sleep(5)
            res = self.sc.parallelize([1], 1).map(lambda x: 0 if x == 0 else b.value[0]).collect()
            self.assertEqual([100], res)
        finally:
            b.destroy()

    def test_broadcast_when_sc_none(self):
        # SPARK-39029 : Test case to improve test coverage of broadcast.py
        # It tests the case when SparkContext is none and Broadcast is called at executor
        conf = SparkConf()
        conf.setMaster("local-cluster[2,1,1024]")
        self.sc = SparkContext(conf=conf)
        bs = self.sc.broadcast([10])
        bs_sc_none = Broadcast(sc=None, path=bs._path)
        self.assertEqual(bs_sc_none.value, [10])

    def test_broadcast_for_error_condition(self):
        # SPARK-39029: Test case to improve test coverage of broadcast.py
        # It tests the case when broadcast should raise error .
        conf = SparkConf()
        conf.setMaster("local-cluster[2,1,1024]")
        self.sc = SparkContext(conf=conf)
        bs = self.sc.broadcast([1])
        with self.assertRaisesRegex(pickle.PickleError, "Could.*not.*serialize.*broadcast"):
            self.sc.broadcast(self.sc)
        with self.assertRaisesRegex(Py4JJavaError, "RuntimeError.*Broadcast.*destroyed.*driver"):
            self.sc.parallelize([1]).map(lambda x: bs.destroy()).collect()
        with self.assertRaisesRegex(Py4JJavaError, "RuntimeError.*Broadcast.*unpersisted.*driver"):
            self.sc.parallelize([1]).map(lambda x: bs.unpersist()).collect()

    def test_broadcast_in_udfs_with_encryption(self):
        conf = SparkConf()
        conf.set("spark.io.encryption.enabled", "true")
        conf.setMaster("local-cluster[2,1,1024]")
        self.sc = SparkContext(conf=conf)
        bar = {"a": "aa", "b": "bb"}
        foo = self.sc.broadcast(bar)
        spark = SparkSession(self.sc)
        spark.udf.register("MYUDF", lambda x: foo.value[x] if x else "")
        sel = spark.sql("SELECT MYUDF('a') AS a, MYUDF('b') AS b")
        self.assertEqual(sel.collect(), [Row(a="aa", b="bb")])
        spark.stop()

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
            self.sc.broadcast(_ArrowBroadcastValue([8, 9]), useArrow=True),
            self.sc.broadcast(_ArrowStreamBroadcastValue([10, 11]), useArrow=True),
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

    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
    def test_arrow_broadcast(self):
        self._test_arrow_broadcast()

    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
    def test_arrow_broadcast_with_encryption(self):
        self._test_arrow_broadcast(("spark.io.encryption.enabled", "true"))

    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
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
class BroadcastArrowSerializationTest(unittest.TestCase):
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
            _ArrowBroadcastValue([1, 2]),
            _ArrowStreamBroadcastValue([3, 4]),
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
            (_ArrowBroadcastWithoutReconstructor([4, 5]), True),
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
        value = _ArrowBroadcastValue([1, 2])
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


class BroadcastFrameProtocolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        gateway = launch_gateway(SparkConf())
        cls._jvm = gateway.jvm
        cls.longMessage = True
        random.seed(42)

    def _test_chunked_stream(self, data, py_buf_size):
        # write data using the chunked protocol from python.
        chunked_file = tempfile.NamedTemporaryFile(delete=False)
        dechunked_file = tempfile.NamedTemporaryFile(delete=False)
        dechunked_file.close()
        try:
            out = ChunkedStream(chunked_file, py_buf_size)
            out.write(data)
            out.close()
            # now try to read it in java
            jin = self._jvm.java.io.FileInputStream(chunked_file.name)
            jout = self._jvm.java.io.FileOutputStream(dechunked_file.name)
            self._jvm.DechunkedInputStream.dechunkAndCopyToOutput(jin, jout)
            # java should have decoded it back to the original data
            self.assertEqual(len(data), os.stat(dechunked_file.name).st_size)
            with open(dechunked_file.name, "rb") as f:
                byte = f.read(1)
                idx = 0
                while byte:
                    self.assertEqual(data[idx], bytearray(byte)[0], msg="idx = " + str(idx))
                    byte = f.read(1)
                    idx += 1
        finally:
            os.unlink(chunked_file.name)
            os.unlink(dechunked_file.name)

    def test_chunked_stream(self):
        def random_bytes(n):
            return bytearray(random.getrandbits(8) for _ in range(n))

        for data_length in [1, 10, 100, 10000]:
            for buffer_length in [1, 2, 5, 8192]:
                self._test_chunked_stream(random_bytes(data_length), buffer_length)


if __name__ == "__main__":
    from pyspark.testing import main

    main()

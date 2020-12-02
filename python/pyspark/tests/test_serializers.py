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
import math
import sys
import unittest

from pyspark import serializers
from pyspark.serializers import CloudPickleSerializer, CompressedSerializer, \
    AutoBatchedSerializer, BatchedSerializer, AutoSerializer, NoOpSerializer, PairDeserializer, \
    FlattenedValuesSerializer, CartesianDeserializer, PickleSerializer, UTF8Deserializer, \
    MarshalSerializer
from pyspark.testing.utils import PySparkTestCase, read_int, write_int, ByteArrayOutput, \
    have_numpy, have_scipy


class SerializationTestCase(unittest.TestCase):

    def test_namedtuple(self):
        from collections import namedtuple
        from pickle import dumps, loads
        P = namedtuple("P", "x y")
        p1 = P(1, 3)
        p2 = loads(dumps(p1, 2))
        self.assertEqual(p1, p2)

        from pyspark.cloudpickle import dumps
        P2 = loads(dumps(P))
        p3 = P2(1, 3)
        self.assertEqual(p1, p3)

    def test_itemgetter(self):
        from operator import itemgetter
        ser = CloudPickleSerializer()
        d = range(10)
        getter = itemgetter(1)
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))

        getter = itemgetter(0, 3)
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))

    def test_function_module_name(self):
        ser = CloudPickleSerializer()
        func = lambda x: x
        func2 = ser.loads(ser.dumps(func))
        self.assertEqual(func.__module__, func2.__module__)

    def test_attrgetter(self):
        from operator import attrgetter
        ser = CloudPickleSerializer()

        class C(object):
            def __getattr__(self, item):
                return item
        d = C()
        getter = attrgetter("a")
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))
        getter = attrgetter("a", "b")
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))

        d.e = C()
        getter = attrgetter("e.a")
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))
        getter = attrgetter("e.a", "e.b")
        getter2 = ser.loads(ser.dumps(getter))
        self.assertEqual(getter(d), getter2(d))

    # Regression test for SPARK-3415
    def test_pickling_file_handles(self):
        # to be corrected with SPARK-11160
        try:
            import xmlrunner  # type: ignore[import]  # noqa: F401
        except ImportError:
            ser = CloudPickleSerializer()
            out1 = sys.stderr
            out2 = ser.loads(ser.dumps(out1))
            self.assertEqual(out1, out2)

    def test_func_globals(self):

        class Unpicklable(object):
            def __reduce__(self):
                raise Exception("not picklable")

        global exit
        exit = Unpicklable()

        ser = CloudPickleSerializer()
        self.assertRaises(Exception, lambda: ser.dumps(exit))

        def foo():
            sys.exit(0)

        self.assertTrue("exit" in foo.__code__.co_names)
        ser.dumps(foo)

    def test_compressed_serializer(self):
        ser = CompressedSerializer(PickleSerializer())
        from io import BytesIO as StringIO
        io = StringIO()
        ser.dump_stream(["abc", u"123", range(5)], io)
        io.seek(0)
        self.assertEqual(["abc", u"123", range(5)], list(ser.load_stream(io)))
        ser.dump_stream(range(1000), io)
        io.seek(0)
        self.assertEqual(["abc", u"123", range(5)] + list(range(1000)), list(ser.load_stream(io)))
        io.close()

    def test_hash_serializer(self):
        hash(NoOpSerializer())
        hash(UTF8Deserializer())
        hash(PickleSerializer())
        hash(MarshalSerializer())
        hash(AutoSerializer())
        hash(BatchedSerializer(PickleSerializer()))
        hash(AutoBatchedSerializer(MarshalSerializer()))
        hash(PairDeserializer(NoOpSerializer(), UTF8Deserializer()))
        hash(CartesianDeserializer(NoOpSerializer(), UTF8Deserializer()))
        hash(CompressedSerializer(PickleSerializer()))
        hash(FlattenedValuesSerializer(PickleSerializer()))


@unittest.skipIf(not have_scipy, "SciPy not installed")
class SciPyTests(PySparkTestCase):

    """General PySpark tests that depend on scipy """

    def test_serialize(self):
        from scipy.special import gammaln

        x = range(1, 5)
        expected = list(map(gammaln, x))
        observed = self.sc.parallelize(x).map(gammaln).collect()
        self.assertEqual(expected, observed)


@unittest.skipIf(not have_numpy, "NumPy not installed")
class NumPyTests(PySparkTestCase):

    """General PySpark tests that depend on numpy """

    def test_statcounter_array(self):
        import numpy as np

        x = self.sc.parallelize([np.array([1.0, 1.0]), np.array([2.0, 2.0]), np.array([3.0, 3.0])])
        s = x.stats()
        self.assertSequenceEqual([2.0, 2.0], s.mean().tolist())
        self.assertSequenceEqual([1.0, 1.0], s.min().tolist())
        self.assertSequenceEqual([3.0, 3.0], s.max().tolist())
        self.assertSequenceEqual([1.0, 1.0], s.sampleStdev().tolist())

        stats_dict = s.asDict()
        self.assertEqual(3, stats_dict['count'])
        self.assertSequenceEqual([2.0, 2.0], stats_dict['mean'].tolist())
        self.assertSequenceEqual([1.0, 1.0], stats_dict['min'].tolist())
        self.assertSequenceEqual([3.0, 3.0], stats_dict['max'].tolist())
        self.assertSequenceEqual([6.0, 6.0], stats_dict['sum'].tolist())
        self.assertSequenceEqual([1.0, 1.0], stats_dict['stdev'].tolist())
        self.assertSequenceEqual([1.0, 1.0], stats_dict['variance'].tolist())

        stats_sample_dict = s.asDict(sample=True)
        self.assertEqual(3, stats_dict['count'])
        self.assertSequenceEqual([2.0, 2.0], stats_sample_dict['mean'].tolist())
        self.assertSequenceEqual([1.0, 1.0], stats_sample_dict['min'].tolist())
        self.assertSequenceEqual([3.0, 3.0], stats_sample_dict['max'].tolist())
        self.assertSequenceEqual([6.0, 6.0], stats_sample_dict['sum'].tolist())
        self.assertSequenceEqual(
            [0.816496580927726, 0.816496580927726], stats_sample_dict['stdev'].tolist())
        self.assertSequenceEqual(
            [0.6666666666666666, 0.6666666666666666], stats_sample_dict['variance'].tolist())


class SerializersTest(unittest.TestCase):

    def test_chunked_stream(self):
        original_bytes = bytearray(range(100))
        for data_length in [1, 10, 100]:
            for buffer_length in [1, 2, 3, 5, 20, 99, 100, 101, 500]:
                dest = ByteArrayOutput()
                stream_out = serializers.ChunkedStream(dest, buffer_length)
                stream_out.write(original_bytes[:data_length])
                stream_out.close()
                num_chunks = int(math.ceil(float(data_length) / buffer_length))
                # length for each chunk, and a final -1 at the very end
                exp_size = (num_chunks + 1) * 4 + data_length
                self.assertEqual(len(dest.buffer), exp_size)
                dest_pos = 0
                data_pos = 0
                for chunk_idx in range(num_chunks):
                    chunk_length = read_int(dest.buffer[dest_pos:(dest_pos + 4)])
                    if chunk_idx == num_chunks - 1:
                        exp_length = data_length % buffer_length
                        if exp_length == 0:
                            exp_length = buffer_length
                    else:
                        exp_length = buffer_length
                    self.assertEqual(chunk_length, exp_length)
                    dest_pos += 4
                    dest_chunk = dest.buffer[dest_pos:dest_pos + chunk_length]
                    orig_chunk = original_bytes[data_pos:data_pos + chunk_length]
                    self.assertEqual(dest_chunk, orig_chunk)
                    dest_pos += chunk_length
                    data_pos += chunk_length
                # ends with a -1
                self.assertEqual(dest.buffer[-4:], write_int(-1))


if __name__ == "__main__":
    from pyspark.tests.test_serializers import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

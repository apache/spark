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
import operator
import os
import shutil
import tempfile
import time
import unittest
from functools import reduce
from itertools import chain
import platform

from pyspark import SparkConf, SparkContext, RDD
from pyspark.streaming import StreamingContext
from pyspark.testing.streamingutils import PySparkStreamingTestCase


@unittest.skipIf(
    "pypy" in platform.python_implementation().lower(),
    "The tests fail in PyPy3 implementation for an unknown reason. "
    "With PyPy, it causes to hang DStream tests forever when Coverage report is used.")
class BasicOperationTests(PySparkStreamingTestCase):

    def test_map(self):
        """Basic operation test for DStream.map."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.map(str)
        expected = [list(map(str, x)) for x in input]
        self._test_func(input, func, expected)

    def test_flatMap(self):
        """Basic operation test for DStream.flatMap."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.flatMap(lambda x: (x, x * 2))
        expected = [list(chain.from_iterable((map(lambda y: [y, y * 2], x))))
                    for x in input]
        self._test_func(input, func, expected)

    def test_filter(self):
        """Basic operation test for DStream.filter."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.filter(lambda x: x % 2 == 0)
        expected = [[y for y in x if y % 2 == 0] for x in input]
        self._test_func(input, func, expected)

    def test_count(self):
        """Basic operation test for DStream.count."""
        input = [range(5), range(10), range(20)]

        def func(dstream):
            return dstream.count()
        expected = [[len(x)] for x in input]
        self._test_func(input, func, expected)

    def test_slice(self):
        """Basic operation test for DStream.slice."""
        import datetime as dt
        self.ssc = StreamingContext(self.sc, 1.0)
        self.ssc.remember(4.0)
        input = [[1], [2], [3], [4]]
        stream = self.ssc.queueStream([self.sc.parallelize(d, 1) for d in input])

        time_vals = []

        def get_times(t, rdd):
            if rdd and len(time_vals) < len(input):
                time_vals.append(t)

        stream.foreachRDD(get_times)

        self.ssc.start()
        self.wait_for(time_vals, 4)
        begin_time = time_vals[0]

        def get_sliced(begin_delta, end_delta):
            begin = begin_time + dt.timedelta(seconds=begin_delta)
            end = begin_time + dt.timedelta(seconds=end_delta)
            rdds = stream.slice(begin, end)
            result_list = [rdd.collect() for rdd in rdds]
            return [r for result in result_list for r in result]

        self.assertEqual(set([1]), set(get_sliced(0, 0)))
        self.assertEqual(set([2, 3]), set(get_sliced(1, 2)))
        self.assertEqual(set([2, 3, 4]), set(get_sliced(1, 4)))
        self.assertEqual(set([1, 2, 3, 4]), set(get_sliced(0, 4)))

    def test_reduce(self):
        """Basic operation test for DStream.reduce."""
        input = [range(1, 5), range(5, 9), range(9, 13)]

        def func(dstream):
            return dstream.reduce(operator.add)
        expected = [[reduce(operator.add, x)] for x in input]
        self._test_func(input, func, expected)

    def test_reduceByKey(self):
        """Basic operation test for DStream.reduceByKey."""
        input = [[("a", 1), ("a", 1), ("b", 1), ("b", 1)],
                 [("", 1), ("", 1), ("", 1), ("", 1)],
                 [(1, 1), (1, 1), (2, 1), (2, 1), (3, 1)]]

        def func(dstream):
            return dstream.reduceByKey(operator.add)
        expected = [[("a", 2), ("b", 2)], [("", 4)], [(1, 2), (2, 2), (3, 1)]]
        self._test_func(input, func, expected, sort=True)

    def test_mapValues(self):
        """Basic operation test for DStream.mapValues."""
        input = [[("a", 2), ("b", 2), ("c", 1), ("d", 1)],
                 [(0, 4), (1, 1), (2, 2), (3, 3)],
                 [(1, 1), (2, 1), (3, 1), (4, 1)]]

        def func(dstream):
            return dstream.mapValues(lambda x: x + 10)
        expected = [[("a", 12), ("b", 12), ("c", 11), ("d", 11)],
                    [(0, 14), (1, 11), (2, 12), (3, 13)],
                    [(1, 11), (2, 11), (3, 11), (4, 11)]]
        self._test_func(input, func, expected, sort=True)

    def test_flatMapValues(self):
        """Basic operation test for DStream.flatMapValues."""
        input = [[("a", 2), ("b", 2), ("c", 1), ("d", 1)],
                 [(0, 4), (1, 1), (2, 1), (3, 1)],
                 [(1, 1), (2, 1), (3, 1), (4, 1)]]

        def func(dstream):
            return dstream.flatMapValues(lambda x: (x, x + 10))
        expected = [[("a", 2), ("a", 12), ("b", 2), ("b", 12),
                     ("c", 1), ("c", 11), ("d", 1), ("d", 11)],
                    [(0, 4), (0, 14), (1, 1), (1, 11), (2, 1), (2, 11), (3, 1), (3, 11)],
                    [(1, 1), (1, 11), (2, 1), (2, 11), (3, 1), (3, 11), (4, 1), (4, 11)]]
        self._test_func(input, func, expected)

    def test_glom(self):
        """Basic operation test for DStream.glom."""
        input = [range(1, 5), range(5, 9), range(9, 13)]
        rdds = [self.sc.parallelize(r, 2) for r in input]

        def func(dstream):
            return dstream.glom()
        expected = [[[1, 2], [3, 4]], [[5, 6], [7, 8]], [[9, 10], [11, 12]]]
        self._test_func(rdds, func, expected)

    def test_mapPartitions(self):
        """Basic operation test for DStream.mapPartitions."""
        input = [range(1, 5), range(5, 9), range(9, 13)]
        rdds = [self.sc.parallelize(r, 2) for r in input]

        def func(dstream):
            def f(iterator):
                yield sum(iterator)
            return dstream.mapPartitions(f)
        expected = [[3, 7], [11, 15], [19, 23]]
        self._test_func(rdds, func, expected)

    def test_countByValue(self):
        """Basic operation test for DStream.countByValue."""
        input = [list(range(1, 5)) * 2, list(range(5, 7)) + list(range(5, 9)), ["a", "a", "b", ""]]

        def func(dstream):
            return dstream.countByValue()
        expected = [[(1, 2), (2, 2), (3, 2), (4, 2)],
                    [(5, 2), (6, 2), (7, 1), (8, 1)],
                    [("a", 2), ("b", 1), ("", 1)]]
        self._test_func(input, func, expected, sort=True)

    def test_groupByKey(self):
        """Basic operation test for DStream.groupByKey."""
        input = [[(1, 1), (2, 1), (3, 1), (4, 1)],
                 [(1, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1)],
                 [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 1), ("", 1)]]

        def func(dstream):
            return dstream.groupByKey().mapValues(list)

        expected = [[(1, [1]), (2, [1]), (3, [1]), (4, [1])],
                    [(1, [1, 1, 1]), (2, [1, 1]), (3, [1])],
                    [("a", [1, 1]), ("b", [1]), ("", [1, 1, 1])]]
        self._test_func(input, func, expected, sort=True)

    def test_combineByKey(self):
        """Basic operation test for DStream.combineByKey."""
        input = [[(1, 1), (2, 1), (3, 1), (4, 1)],
                 [(1, 1), (1, 1), (1, 1), (2, 1), (2, 1), (3, 1)],
                 [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 1), ("", 1)]]

        def func(dstream):
            def add(a, b):
                return a + str(b)
            return dstream.combineByKey(str, add, add)
        expected = [[(1, "1"), (2, "1"), (3, "1"), (4, "1")],
                    [(1, "111"), (2, "11"), (3, "1")],
                    [("a", "11"), ("b", "1"), ("", "111")]]
        self._test_func(input, func, expected, sort=True)

    def test_repartition(self):
        input = [range(1, 5), range(5, 9)]
        rdds = [self.sc.parallelize(r, 2) for r in input]

        def func(dstream):
            return dstream.repartition(1).glom()
        expected = [[[1, 2, 3, 4]], [[5, 6, 7, 8]]]
        self._test_func(rdds, func, expected)

    def test_union(self):
        input1 = [range(3), range(5), range(6)]
        input2 = [range(3, 6), range(5, 6)]

        def func(d1, d2):
            return d1.union(d2)

        expected = [list(range(6)), list(range(6)), list(range(6))]
        self._test_func(input1, func, expected, input2=input2)

    def test_cogroup(self):
        input = [[(1, 1), (2, 1), (3, 1)],
                 [(1, 1), (1, 1), (1, 1), (2, 1)],
                 [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 1)]]
        input2 = [[(1, 2)],
                  [(4, 1)],
                  [("a", 1), ("a", 1), ("b", 1), ("", 1), ("", 2)]]

        def func(d1, d2):
            return d1.cogroup(d2).mapValues(lambda vs: tuple(map(list, vs)))

        expected = [[(1, ([1], [2])), (2, ([1], [])), (3, ([1], []))],
                    [(1, ([1, 1, 1], [])), (2, ([1], [])), (4, ([], [1]))],
                    [("a", ([1, 1], [1, 1])), ("b", ([1], [1])), ("", ([1, 1], [1, 2]))]]
        self._test_func(input, func, expected, sort=True, input2=input2)

    def test_join(self):
        input = [[('a', 1), ('b', 2)]]
        input2 = [[('b', 3), ('c', 4)]]

        def func(a, b):
            return a.join(b)

        expected = [[('b', (2, 3))]]
        self._test_func(input, func, expected, True, input2)

    def test_left_outer_join(self):
        input = [[('a', 1), ('b', 2)]]
        input2 = [[('b', 3), ('c', 4)]]

        def func(a, b):
            return a.leftOuterJoin(b)

        expected = [[('a', (1, None)), ('b', (2, 3))]]
        self._test_func(input, func, expected, True, input2)

    def test_right_outer_join(self):
        input = [[('a', 1), ('b', 2)]]
        input2 = [[('b', 3), ('c', 4)]]

        def func(a, b):
            return a.rightOuterJoin(b)

        expected = [[('b', (2, 3)), ('c', (None, 4))]]
        self._test_func(input, func, expected, True, input2)

    def test_full_outer_join(self):
        input = [[('a', 1), ('b', 2)]]
        input2 = [[('b', 3), ('c', 4)]]

        def func(a, b):
            return a.fullOuterJoin(b)

        expected = [[('a', (1, None)), ('b', (2, 3)), ('c', (None, 4))]]
        self._test_func(input, func, expected, True, input2)

    def test_update_state_by_key(self):

        def updater(vs, s):
            if not s:
                s = []
            s.extend(vs)
            return s

        input = [[('k', i)] for i in range(5)]

        def func(dstream):
            return dstream.updateStateByKey(updater)

        expected = [[0], [0, 1], [0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4]]
        expected = [[('k', v)] for v in expected]
        self._test_func(input, func, expected)

    def test_update_state_by_key_initial_rdd(self):

        def updater(vs, s):
            if not s:
                s = []
            s.extend(vs)
            return s

        initial = [('k', [0, 1])]
        initial = self.sc.parallelize(initial, 1)

        input = [[('k', i)] for i in range(2, 5)]

        def func(dstream):
            return dstream.updateStateByKey(updater, initialRDD=initial)

        expected = [[0, 1, 2], [0, 1, 2, 3], [0, 1, 2, 3, 4]]
        expected = [[('k', v)] for v in expected]
        self._test_func(input, func, expected)

    def test_failed_func(self):
        # Test failure in
        # TransformFunction.apply(rdd: Option[RDD[_]], time: Time)
        input = [self.sc.parallelize([d], 1) for d in range(4)]
        input_stream = self.ssc.queueStream(input)

        def failed_func(i):
            raise ValueError("This is a special error")

        input_stream.map(failed_func).pprint()
        self.ssc.start()
        try:
            self.ssc.awaitTerminationOrTimeout(10)
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue("This is a special error" in failure)
            return

        self.fail("a failed func should throw an error")

    def test_failed_func2(self):
        # Test failure in
        # TransformFunction.apply(rdd: Option[RDD[_]], rdd2: Option[RDD[_]], time: Time)
        input = [self.sc.parallelize([d], 1) for d in range(4)]
        input_stream1 = self.ssc.queueStream(input)
        input_stream2 = self.ssc.queueStream(input)

        def failed_func(rdd1, rdd2):
            raise ValueError("This is a special error")

        input_stream1.transformWith(failed_func, input_stream2, True).pprint()
        self.ssc.start()
        try:
            self.ssc.awaitTerminationOrTimeout(10)
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue("This is a special error" in failure)
            return

        self.fail("a failed func should throw an error")

    def test_failed_func_with_reseting_failure(self):
        input = [self.sc.parallelize([d], 1) for d in range(4)]
        input_stream = self.ssc.queueStream(input)

        def failed_func(i):
            if i == 1:
                # Make it fail in the second batch
                raise ValueError("This is a special error")
            else:
                return i

        # We should be able to see the results of the 3rd and 4th batches even if the second batch
        # fails
        expected = [[0], [2], [3]]
        self.assertEqual(expected, self._collect(input_stream.map(failed_func), 3))
        try:
            self.ssc.awaitTerminationOrTimeout(10)
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue("This is a special error" in failure)
            return

        self.fail("a failed func should throw an error")


@unittest.skipIf(
    "pypy" in platform.python_implementation().lower(),
    "The tests fail in PyPy3 implementation for an unknown reason. "
    "With PyPy, it causes to hang DStream tests forever when Coverage report is used.")
class WindowFunctionTests(PySparkStreamingTestCase):

    timeout = 15

    def test_window(self):
        input = [range(1), range(2), range(3), range(4), range(5)]

        def func(dstream):
            return dstream.window(1.5, .5).count()

        expected = [[1], [3], [6], [9], [12], [9], [5]]
        self._test_func(input, func, expected)

    def test_count_by_window(self):
        input = [range(1), range(2), range(3), range(4), range(5)]

        def func(dstream):
            return dstream.countByWindow(1.5, .5)

        expected = [[1], [3], [6], [9], [12], [9], [5]]
        self._test_func(input, func, expected)

    def test_count_by_window_large(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.countByWindow(2.5, .5)

        expected = [[1], [3], [6], [10], [15], [20], [18], [15], [11], [6]]
        self._test_func(input, func, expected)

    def test_count_by_value_and_window(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.countByValueAndWindow(2.5, .5)

        expected = [[(0, 1)],
                    [(0, 2), (1, 1)],
                    [(0, 3), (1, 2), (2, 1)],
                    [(0, 4), (1, 3), (2, 2), (3, 1)],
                    [(0, 5), (1, 4), (2, 3), (3, 2), (4, 1)],
                    [(0, 5), (1, 5), (2, 4), (3, 3), (4, 2), (5, 1)],
                    [(0, 4), (1, 4), (2, 4), (3, 3), (4, 2), (5, 1)],
                    [(0, 3), (1, 3), (2, 3), (3, 3), (4, 2), (5, 1)],
                    [(0, 2), (1, 2), (2, 2), (3, 2), (4, 2), (5, 1)],
                    [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]]
        self._test_func(input, func, expected)

    def test_group_by_key_and_window(self):
        input = [[('a', i)] for i in range(5)]

        def func(dstream):
            return dstream.groupByKeyAndWindow(1.5, .5).mapValues(list)

        expected = [[('a', [0])], [('a', [0, 1])], [('a', [0, 1, 2])], [('a', [1, 2, 3])],
                    [('a', [2, 3, 4])], [('a', [3, 4])], [('a', [4])]]
        self._test_func(input, func, expected)

    def test_reduce_by_invalid_window(self):
        input1 = [range(3), range(5), range(1), range(6)]
        d1 = self.ssc.queueStream(input1)
        self.assertRaises(ValueError, lambda: d1.reduceByKeyAndWindow(None, None, 0.1, 0.1))
        self.assertRaises(ValueError, lambda: d1.reduceByKeyAndWindow(None, None, 1, 0.1))

    def test_reduce_by_key_and_window_with_none_invFunc(self):
        input = [range(1), range(2), range(3), range(4), range(5), range(6)]

        def func(dstream):
            return dstream.map(lambda x: (x, 1))\
                .reduceByKeyAndWindow(operator.add, None, 5, 1)\
                .filter(lambda kv: kv[1] > 0).count()

        expected = [[2], [4], [6], [6], [6], [6]]
        self._test_func(input, func, expected)


@unittest.skipIf(
    "pypy" in platform.python_implementation().lower(),
    "The tests fail in PyPy3 implementation for an unknown reason. "
    "With PyPy, it causes to hang DStream tests forever when Coverage report is used.")
class CheckpointTests(unittest.TestCase):

    setupCalled = False

    @staticmethod
    def tearDownClass():
        # Clean up in the JVM just in case there has been some issues in Python API
        if SparkContext._jvm is not None:
            jStreamingContextOption = \
                SparkContext._jvm.org.apache.spark.streaming.StreamingContext.getActive()
            if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop()

    def setUp(self):
        self.ssc = None
        self.sc = None
        self.cpd = None

    def tearDown(self):
        if self.ssc is not None:
            self.ssc.stop(True)
        if self.sc is not None:
            self.sc.stop()
        if self.cpd is not None:
            shutil.rmtree(self.cpd)

    def test_transform_function_serializer_failure(self):
        inputd = tempfile.mkdtemp()
        self.cpd = tempfile.mkdtemp("test_transform_function_serializer_failure")

        def setup():
            conf = SparkConf().set("spark.default.parallelism", 1)
            sc = SparkContext(conf=conf)
            ssc = StreamingContext(sc, 0.5)

            # A function that cannot be serialized
            def process(time, rdd):
                sc.parallelize(range(1, 10))

            ssc.textFileStream(inputd).foreachRDD(process)
            return ssc

        self.ssc = StreamingContext.getOrCreate(self.cpd, setup)
        try:
            self.ssc.start()
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue(
                "It appears that you are attempting to reference SparkContext" in failure)
            return

        self.fail("using SparkContext in process should fail because it's not Serializable")

    def test_get_or_create_and_get_active_or_create(self):
        inputd = tempfile.mkdtemp()
        outputd = tempfile.mkdtemp() + "/"

        def updater(vs, s):
            return sum(vs, s or 0)

        def setup():
            conf = SparkConf().set("spark.default.parallelism", 1)
            sc = SparkContext(conf=conf)
            ssc = StreamingContext(sc, 2)
            dstream = ssc.textFileStream(inputd).map(lambda x: (x, 1))
            wc = dstream.updateStateByKey(updater)
            wc.map(lambda x: "%s,%d" % x).saveAsTextFiles(outputd + "test")
            wc.checkpoint(2)
            self.setupCalled = True
            return ssc

        # Verify that getOrCreate() calls setup() in absence of checkpoint files
        self.cpd = tempfile.mkdtemp("test_streaming_cps")
        self.setupCalled = False
        self.ssc = StreamingContext.getOrCreate(self.cpd, setup)
        self.assertTrue(self.setupCalled)

        self.ssc.start()

        def check_output(n):
            while not os.listdir(outputd):
                if self.ssc.awaitTerminationOrTimeout(0.5):
                    raise Exception("ssc stopped")
            time.sleep(1)  # make sure mtime is larger than the previous one
            with open(os.path.join(inputd, str(n)), 'w') as f:
                f.writelines(["%d\n" % i for i in range(10)])

            while True:
                if self.ssc.awaitTerminationOrTimeout(0.5):
                    raise Exception("ssc stopped")
                p = os.path.join(outputd, max(os.listdir(outputd)))
                if '_SUCCESS' not in os.listdir(p):
                    # not finished
                    continue
                ordd = self.ssc.sparkContext.textFile(p).map(lambda line: line.split(","))
                d = ordd.values().map(int).collect()
                if not d:
                    continue
                self.assertEqual(10, len(d))
                s = set(d)
                self.assertEqual(1, len(s))
                m = s.pop()
                if n > m:
                    continue
                self.assertEqual(n, m)
                break

        check_output(1)
        check_output(2)

        # Verify the getOrCreate() recovers from checkpoint files
        self.ssc.stop(True, True)
        time.sleep(1)
        self.setupCalled = False
        self.ssc = StreamingContext.getOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.ssc.start()
        check_output(3)

        # Verify that getOrCreate() uses existing SparkContext
        self.ssc.stop(True, True)
        time.sleep(1)
        self.sc = SparkContext(conf=SparkConf())
        self.setupCalled = False
        self.ssc = StreamingContext.getOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.assertTrue(self.ssc.sparkContext == self.sc)

        # Verify the getActiveOrCreate() recovers from checkpoint files
        self.ssc.stop(True, True)
        time.sleep(1)
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.ssc.start()
        check_output(4)

        # Verify that getActiveOrCreate() returns active context
        self.setupCalled = False
        self.assertEqual(StreamingContext.getActiveOrCreate(self.cpd, setup), self.ssc)
        self.assertFalse(self.setupCalled)

        # Verify that getActiveOrCreate() uses existing SparkContext
        self.ssc.stop(True, True)
        time.sleep(1)
        self.sc = SparkContext(conf=SparkConf())
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.assertTrue(self.ssc.sparkContext == self.sc)

        # Verify that getActiveOrCreate() calls setup() in absence of checkpoint files
        self.ssc.stop(True, True)
        shutil.rmtree(self.cpd)  # delete checkpoint directory
        time.sleep(1)
        self.setupCalled = False
        self.ssc = StreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertTrue(self.setupCalled)

        # Stop everything
        self.ssc.stop(True, True)


if __name__ == "__main__":
    from pyspark.streaming.tests.test_dstream import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

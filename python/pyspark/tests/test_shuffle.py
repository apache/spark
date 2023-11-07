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
import random
import unittest
import tempfile
import os

from py4j.protocol import Py4JJavaError

from pyspark import shuffle, CPickleSerializer, SparkConf, SparkContext
from pyspark.shuffle import (
    Aggregator,
    ExternalMerger,
    ExternalSorter,
    SimpleAggregator,
    Merger,
    ExternalGroupBy,
)


class MergerTests(unittest.TestCase):
    def setUp(self):
        self.N = 1 << 12
        self.lst = [i for i in range(self.N)]
        self.data = list(zip(self.lst, self.lst))
        self.agg = Aggregator(
            lambda x: [x], lambda x, y: x.append(y) or x, lambda x, y: x.extend(y) or x
        )

    def test_small_dataset(self):
        m = ExternalMerger(self.agg, 1000)
        m.mergeValues(self.data)
        self.assertEqual(m.spills, 0)
        self.assertEqual(sum(sum(v) for k, v in m.items()), sum(range(self.N)))

        m = ExternalMerger(self.agg, 1000)
        m.mergeCombiners(map(lambda x_y1: (x_y1[0], [x_y1[1]]), self.data))
        self.assertEqual(m.spills, 0)
        self.assertEqual(sum(sum(v) for k, v in m.items()), sum(range(self.N)))

    def test_medium_dataset(self):
        m = ExternalMerger(self.agg, 20)
        m.mergeValues(self.data)
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.items()), sum(range(self.N)))

        m = ExternalMerger(self.agg, 10)
        m.mergeCombiners(map(lambda x_y2: (x_y2[0], [x_y2[1]]), self.data * 3))
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.items()), sum(range(self.N)) * 3)

    def test_shuffle_data_with_multiple_locations(self):
        # SPARK-39179: Test shuffle of data with multiple location also check
        # shuffle locations get randomized

        with tempfile.TemporaryDirectory() as tempdir1, tempfile.TemporaryDirectory() as tempdir2:
            original = os.environ.get("SPARK_LOCAL_DIRS", None)
            os.environ["SPARK_LOCAL_DIRS"] = tempdir1 + "," + tempdir2
            try:
                index_of_tempdir1 = [False, False]
                for idx in range(10):
                    m = ExternalMerger(self.agg, 20)
                    if m.localdirs[0].startswith(tempdir1):
                        index_of_tempdir1[0] = True
                    elif m.localdirs[1].startswith(tempdir1):
                        index_of_tempdir1[1] = True
                    m.mergeValues(self.data)
                    self.assertTrue(m.spills >= 1)
                    self.assertEqual(sum(sum(v) for k, v in m.items()), sum(range(self.N)))
                self.assertTrue(
                    index_of_tempdir1[0] and (index_of_tempdir1[0] == index_of_tempdir1[1])
                )
            finally:
                if original is not None:
                    os.environ["SPARK_LOCAL_DIRS"] = original
                else:
                    del os.environ["SPARK_LOCAL_DIRS"]

    def test_simple_aggregator_with_medium_dataset(self):
        # SPARK-39179: Test Simple aggregator
        agg = SimpleAggregator(lambda x, y: x + y)
        m = ExternalMerger(agg, 20)
        m.mergeValues(self.data)
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(v for k, v in m.items()), sum(range(self.N)))

    def test_merger_not_implemented_error(self):
        # SPARK-39179: Test Merger for error scenarios
        agg = SimpleAggregator(lambda x, y: x + y)

        class DummyMerger(Merger):
            def __init__(self, agg):
                Merger.__init__(self, agg)

        dummy_merger = DummyMerger(agg)
        with self.assertRaises(NotImplementedError):
            dummy_merger.mergeValues(self.data)
        with self.assertRaises(NotImplementedError):
            dummy_merger.mergeCombiners(self.data)
        with self.assertRaises(NotImplementedError):
            dummy_merger.items()

    def test_huge_dataset(self):
        m = ExternalMerger(self.agg, 5, partitions=3)
        m.mergeCombiners(map(lambda k_v: (k_v[0], [str(k_v[1])]), self.data * 10))
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(len(v) for k, v in m.items()), self.N * 10)
        m._cleanup()

    def test_group_by_key(self):
        def gen_data(N, step):
            for i in range(1, N + 1, step):
                for j in range(i):
                    yield (i, [j])

        def gen_gs(N, step=1):
            return shuffle.GroupByKey(gen_data(N, step))

        self.assertEqual(1, len(list(gen_gs(1))))
        self.assertEqual(2, len(list(gen_gs(2))))
        self.assertEqual(100, len(list(gen_gs(100))))
        self.assertEqual(list(range(1, 101)), [k for k, _ in gen_gs(100)])
        self.assertTrue(all(list(range(k)) == list(vs) for k, vs in gen_gs(100)))

        for k, vs in gen_gs(50002, 10000):
            self.assertEqual(k, len(vs))
            self.assertEqual(list(range(k)), list(vs))

        ser = CPickleSerializer()
        lst = ser.loads(ser.dumps(list(gen_gs(50002, 30000))))
        for k, vs in lst:
            self.assertEqual(k, len(vs))
            self.assertEqual(list(range(k)), list(vs))

    def test_stopiteration_is_raised(self):
        def stopit(*args, **kwargs):
            raise StopIteration()

        def legit_create_combiner(x):
            return [x]

        def legit_merge_value(x, y):
            return x.append(y) or x

        def legit_merge_combiners(x, y):
            return x.extend(y) or x

        data = [(x % 2, x) for x in range(100)]

        # wrong create combiner
        m = ExternalMerger(Aggregator(stopit, legit_merge_value, legit_merge_combiners), 20)
        with self.assertRaises((Py4JJavaError, RuntimeError)):
            m.mergeValues(data)

        # wrong merge value
        m = ExternalMerger(Aggregator(legit_create_combiner, stopit, legit_merge_combiners), 20)
        with self.assertRaises((Py4JJavaError, RuntimeError)):
            m.mergeValues(data)

        # wrong merge combiners
        m = ExternalMerger(Aggregator(legit_create_combiner, legit_merge_value, stopit), 20)
        with self.assertRaises((Py4JJavaError, RuntimeError)):
            m.mergeCombiners(map(lambda x_y1: (x_y1[0], [x_y1[1]]), data))


class ExternalGroupByTests(unittest.TestCase):
    def setUp(self):
        self.N = 1 << 20
        values = [i for i in range(self.N)]
        keys = [i for i in range(2)]
        import itertools

        self.data = [value for value in itertools.product(keys, values)]
        self.agg = Aggregator(
            lambda x: [x], lambda x, y: x.append(y) or x, lambda x, y: x.extend(y) or x
        )

    def test_medium_dataset(self):
        # SPARK-39179: Test external group by for medium dataset
        m = ExternalGroupBy(self.agg, 5, partitions=3)
        m.mergeValues(self.data)
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.items()), 2 * sum(range(self.N)))

    def test_dataset_with_keys_are_unsorted(self):
        # SPARK-39179: Test external group when numbers of keys are greater than SORT KEY Limit.
        m = ExternalGroupBy(self.agg, 5, partitions=3)
        original = m.SORT_KEY_LIMIT
        try:
            m.SORT_KEY_LIMIT = 1
            m.mergeValues(self.data)
            self.assertTrue(m.spills >= 1)
            self.assertEqual(sum(sum(v) for k, v in m.items()), 2 * sum(range(self.N)))
        finally:
            m.SORT_KEY_LIMIT = original


class SorterTests(unittest.TestCase):
    def test_in_memory_sort(self):
        lst = list(range(1024))
        random.shuffle(lst)
        sorter = ExternalSorter(1024)
        self.assertEqual(sorted(lst), list(sorter.sorted(lst)))
        self.assertEqual(sorted(lst, reverse=True), list(sorter.sorted(lst, reverse=True)))
        self.assertEqual(sorted(lst, key=lambda x: -x), list(sorter.sorted(lst, key=lambda x: -x)))
        self.assertEqual(
            sorted(lst, key=lambda x: -x, reverse=True),
            list(sorter.sorted(lst, key=lambda x: -x, reverse=True)),
        )

    def test_external_sort(self):
        class CustomizedSorter(ExternalSorter):
            def _next_limit(self):
                return self.memory_limit

        lst = list(range(1024))
        random.shuffle(lst)
        sorter = CustomizedSorter(1)
        self.assertEqual(sorted(lst), list(sorter.sorted(lst)))
        self.assertGreater(shuffle.DiskBytesSpilled, 0)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(sorted(lst, reverse=True), list(sorter.sorted(lst, reverse=True)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(sorted(lst, key=lambda x: -x), list(sorter.sorted(lst, key=lambda x: -x)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(
            sorted(lst, key=lambda x: -x, reverse=True),
            list(sorter.sorted(lst, key=lambda x: -x, reverse=True)),
        )
        self.assertGreater(shuffle.DiskBytesSpilled, last)

    def test_external_sort_in_rdd(self):
        conf = SparkConf().set("spark.python.worker.memory", "1m")
        sc = SparkContext(conf=conf)
        lst = list(range(10240))
        random.shuffle(lst)
        rdd = sc.parallelize(lst, 4)
        self.assertEqual(sorted(lst), rdd.sortBy(lambda x: x).collect())
        sc.stop()


if __name__ == "__main__":
    from pyspark.tests.test_shuffle import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

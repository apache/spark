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

from py4j.protocol import Py4JJavaError

from pyspark import shuffle, CPickleSerializer, SparkConf, SparkContext
from pyspark.shuffle import Aggregator, ExternalMerger, ExternalSorter


class MergerTests(unittest.TestCase):
    def setUp(self):
        self.N = 1 << 12
        self.l = [i for i in range(self.N)]
        self.data = list(zip(self.l, self.l))
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
        l = ser.loads(ser.dumps(list(gen_gs(50002, 30000))))
        for k, vs in l:
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
        with self.assertRaises((Py4JJavaError, RuntimeError)) as cm:
            m.mergeValues(data)

        # wrong merge value
        m = ExternalMerger(Aggregator(legit_create_combiner, stopit, legit_merge_combiners), 20)
        with self.assertRaises((Py4JJavaError, RuntimeError)) as cm:
            m.mergeValues(data)

        # wrong merge combiners
        m = ExternalMerger(Aggregator(legit_create_combiner, legit_merge_value, stopit), 20)
        with self.assertRaises((Py4JJavaError, RuntimeError)) as cm:
            m.mergeCombiners(map(lambda x_y1: (x_y1[0], [x_y1[1]]), data))


class SorterTests(unittest.TestCase):
    def test_in_memory_sort(self):
        l = list(range(1024))
        random.shuffle(l)
        sorter = ExternalSorter(1024)
        self.assertEqual(sorted(l), list(sorter.sorted(l)))
        self.assertEqual(sorted(l, reverse=True), list(sorter.sorted(l, reverse=True)))
        self.assertEqual(sorted(l, key=lambda x: -x), list(sorter.sorted(l, key=lambda x: -x)))
        self.assertEqual(
            sorted(l, key=lambda x: -x, reverse=True),
            list(sorter.sorted(l, key=lambda x: -x, reverse=True)),
        )

    def test_external_sort(self):
        class CustomizedSorter(ExternalSorter):
            def _next_limit(self):
                return self.memory_limit

        l = list(range(1024))
        random.shuffle(l)
        sorter = CustomizedSorter(1)
        self.assertEqual(sorted(l), list(sorter.sorted(l)))
        self.assertGreater(shuffle.DiskBytesSpilled, 0)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(sorted(l, reverse=True), list(sorter.sorted(l, reverse=True)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(sorted(l, key=lambda x: -x), list(sorter.sorted(l, key=lambda x: -x)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)
        last = shuffle.DiskBytesSpilled
        self.assertEqual(
            sorted(l, key=lambda x: -x, reverse=True),
            list(sorter.sorted(l, key=lambda x: -x, reverse=True)),
        )
        self.assertGreater(shuffle.DiskBytesSpilled, last)

    def test_external_sort_in_rdd(self):
        conf = SparkConf().set("spark.python.worker.memory", "1m")
        sc = SparkContext(conf=conf)
        l = list(range(10240))
        random.shuffle(l)
        rdd = sc.parallelize(l, 4)
        self.assertEqual(sorted(l), rdd.sortBy(lambda x: x).collect())
        sc.stop()


if __name__ == "__main__":
    from pyspark.tests.test_shuffle import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

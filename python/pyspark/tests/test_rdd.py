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
from datetime import datetime, timedelta
import hashlib
import os
import random
import sys
import tempfile
import time
from glob import glob

from py4j.protocol import Py4JJavaError

from pyspark import shuffle, RDD
from pyspark.serializers import CloudPickleSerializer, BatchedSerializer, PickleSerializer,\
    MarshalSerializer, UTF8Deserializer, NoOpSerializer
from pyspark.testing.utils import ReusedPySparkTestCase, SPARK_HOME, QuietTest

if sys.version_info[0] >= 3:
    xrange = range


global_func = lambda: "Hi"


class RDDTests(ReusedPySparkTestCase):

    def test_range(self):
        self.assertEqual(self.sc.range(1, 1).count(), 0)
        self.assertEqual(self.sc.range(1, 0, -1).count(), 1)
        self.assertEqual(self.sc.range(0, 1 << 40, 1 << 39).count(), 2)

    def test_id(self):
        rdd = self.sc.parallelize(range(10))
        id = rdd.id()
        self.assertEqual(id, rdd.id())
        rdd2 = rdd.map(str).filter(bool)
        id2 = rdd2.id()
        self.assertEqual(id + 1, id2)
        self.assertEqual(id2, rdd2.id())

    def test_empty_rdd(self):
        rdd = self.sc.emptyRDD()
        self.assertTrue(rdd.isEmpty())

    def test_sum(self):
        self.assertEqual(0, self.sc.emptyRDD().sum())
        self.assertEqual(6, self.sc.parallelize([1, 2, 3]).sum())

    def test_to_localiterator(self):
        rdd = self.sc.parallelize([1, 2, 3])
        it = rdd.toLocalIterator()
        self.assertEqual([1, 2, 3], sorted(it))

        rdd2 = rdd.repartition(1000)
        it2 = rdd2.toLocalIterator()
        self.assertEqual([1, 2, 3], sorted(it2))

    def test_to_localiterator_prefetch(self):
        # Test that we fetch the next partition in parallel
        # We do this by returning the current time and:
        # reading the first elem, waiting, and reading the second elem
        # If not in parallel then these would be at different times
        # But since they are being computed in parallel we see the time
        # is "close enough" to the same.
        rdd = self.sc.parallelize(range(2), 2)
        times1 = rdd.map(lambda x: datetime.now())
        times2 = rdd.map(lambda x: datetime.now())
        times_iter_prefetch = times1.toLocalIterator(prefetchPartitions=True)
        times_iter = times2.toLocalIterator(prefetchPartitions=False)
        times_prefetch_head = next(times_iter_prefetch)
        times_head = next(times_iter)
        time.sleep(2)
        times_next = next(times_iter)
        times_prefetch_next = next(times_iter_prefetch)
        self.assertTrue(times_next - times_head >= timedelta(seconds=2))
        self.assertTrue(times_prefetch_next - times_prefetch_head < timedelta(seconds=1))

    def test_save_as_textfile_with_unicode(self):
        # Regression test for SPARK-970
        x = u"\u00A1Hola, mundo!"
        data = self.sc.parallelize([x])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsTextFile(tempFile.name)
        raw_contents = b''.join(open(p, 'rb').read()
                                for p in glob(tempFile.name + "/part-0000*"))
        self.assertEqual(x, raw_contents.strip().decode("utf-8"))

    def test_save_as_textfile_with_utf8(self):
        x = u"\u00A1Hola, mundo!"
        data = self.sc.parallelize([x.encode("utf-8")])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsTextFile(tempFile.name)
        raw_contents = b''.join(open(p, 'rb').read()
                                for p in glob(tempFile.name + "/part-0000*"))
        self.assertEqual(x, raw_contents.strip().decode('utf8'))

    def test_transforming_cartesian_result(self):
        # Regression test for SPARK-1034
        rdd1 = self.sc.parallelize([1, 2])
        rdd2 = self.sc.parallelize([3, 4])
        cart = rdd1.cartesian(rdd2)
        result = cart.map(lambda x_y3: x_y3[0] + x_y3[1]).collect()

    def test_transforming_pickle_file(self):
        # Regression test for SPARK-2601
        data = self.sc.parallelize([u"Hello", u"World!"])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsPickleFile(tempFile.name)
        pickled_file = self.sc.pickleFile(tempFile.name)
        pickled_file.map(lambda x: x).collect()

    def test_cartesian_on_textfile(self):
        # Regression test for
        path = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        a = self.sc.textFile(path)
        result = a.cartesian(a).collect()
        (x, y) = result[0]
        self.assertEqual(u"Hello World!", x.strip())
        self.assertEqual(u"Hello World!", y.strip())

    def test_cartesian_chaining(self):
        # Tests for SPARK-16589
        rdd = self.sc.parallelize(range(10), 2)
        self.assertSetEqual(
            set(rdd.cartesian(rdd).cartesian(rdd).collect()),
            set([((x, y), z) for x in range(10) for y in range(10) for z in range(10)])
        )

        self.assertSetEqual(
            set(rdd.cartesian(rdd.cartesian(rdd)).collect()),
            set([(x, (y, z)) for x in range(10) for y in range(10) for z in range(10)])
        )

        self.assertSetEqual(
            set(rdd.cartesian(rdd.zip(rdd)).collect()),
            set([(x, (y, y)) for x in range(10) for y in range(10)])
        )

    def test_zip_chaining(self):
        # Tests for SPARK-21985
        rdd = self.sc.parallelize('abc', 2)
        self.assertSetEqual(
            set(rdd.zip(rdd).zip(rdd).collect()),
            set([((x, x), x) for x in 'abc'])
        )
        self.assertSetEqual(
            set(rdd.zip(rdd.zip(rdd)).collect()),
            set([(x, (x, x)) for x in 'abc'])
        )

    def test_union_pair_rdd(self):
        # SPARK-31788: test if pair RDDs can be combined by union.
        rdd = self.sc.parallelize([1, 2])
        pair_rdd = rdd.zip(rdd)
        unionRDD = self.sc.union([pair_rdd, pair_rdd])
        self.assertEqual(
            set(unionRDD.collect()),
            set([(1, 1), (2, 2), (1, 1), (2, 2)])
        )
        self.assertEqual(unionRDD.count(), 4)

    def test_deleting_input_files(self):
        # Regression test for SPARK-1025
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write(b"Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        with QuietTest(self.sc):
            self.assertRaises(Exception, lambda: filtered_data.count())

    def test_sampling_default_seed(self):
        # Test for SPARK-3995 (default seed setting)
        data = self.sc.parallelize(xrange(1000), 1)
        subset = data.takeSample(False, 10)
        self.assertEqual(len(subset), 10)

    def test_aggregate_mutable_zero_value(self):
        # Test for SPARK-9021; uses aggregate and treeAggregate to build dict
        # representing a counter of ints
        # NOTE: dict is used instead of collections.Counter for Python 2.6
        # compatibility
        from collections import defaultdict

        # Show that single or multiple partitions work
        data1 = self.sc.range(10, numSlices=1)
        data2 = self.sc.range(10, numSlices=2)

        def seqOp(x, y):
            x[y] += 1
            return x

        def comboOp(x, y):
            for key, val in y.items():
                x[key] += val
            return x

        counts1 = data1.aggregate(defaultdict(int), seqOp, comboOp)
        counts2 = data2.aggregate(defaultdict(int), seqOp, comboOp)
        counts3 = data1.treeAggregate(defaultdict(int), seqOp, comboOp, 2)
        counts4 = data2.treeAggregate(defaultdict(int), seqOp, comboOp, 2)

        ground_truth = defaultdict(int, dict((i, 1) for i in range(10)))
        self.assertEqual(counts1, ground_truth)
        self.assertEqual(counts2, ground_truth)
        self.assertEqual(counts3, ground_truth)
        self.assertEqual(counts4, ground_truth)

    def test_aggregate_by_key_mutable_zero_value(self):
        # Test for SPARK-9021; uses aggregateByKey to make a pair RDD that
        # contains lists of all values for each key in the original RDD

        # list(range(...)) for Python 3.x compatibility (can't use * operator
        # on a range object)
        # list(zip(...)) for Python 3.x compatibility (want to parallelize a
        # collection, not a zip object)
        tuples = list(zip(list(range(10))*2, [1]*20))
        # Show that single or multiple partitions work
        data1 = self.sc.parallelize(tuples, 1)
        data2 = self.sc.parallelize(tuples, 2)

        def seqOp(x, y):
            x.append(y)
            return x

        def comboOp(x, y):
            x.extend(y)
            return x

        values1 = data1.aggregateByKey([], seqOp, comboOp).collect()
        values2 = data2.aggregateByKey([], seqOp, comboOp).collect()
        # Sort lists to ensure clean comparison with ground_truth
        values1.sort()
        values2.sort()

        ground_truth = [(i, [1]*2) for i in range(10)]
        self.assertEqual(values1, ground_truth)
        self.assertEqual(values2, ground_truth)

    def test_fold_mutable_zero_value(self):
        # Test for SPARK-9021; uses fold to merge an RDD of dict counters into
        # a single dict
        # NOTE: dict is used instead of collections.Counter for Python 2.6
        # compatibility
        from collections import defaultdict

        counts1 = defaultdict(int, dict((i, 1) for i in range(10)))
        counts2 = defaultdict(int, dict((i, 1) for i in range(3, 8)))
        counts3 = defaultdict(int, dict((i, 1) for i in range(4, 7)))
        counts4 = defaultdict(int, dict((i, 1) for i in range(5, 6)))
        all_counts = [counts1, counts2, counts3, counts4]
        # Show that single or multiple partitions work
        data1 = self.sc.parallelize(all_counts, 1)
        data2 = self.sc.parallelize(all_counts, 2)

        def comboOp(x, y):
            for key, val in y.items():
                x[key] += val
            return x

        fold1 = data1.fold(defaultdict(int), comboOp)
        fold2 = data2.fold(defaultdict(int), comboOp)

        ground_truth = defaultdict(int)
        for counts in all_counts:
            for key, val in counts.items():
                ground_truth[key] += val
        self.assertEqual(fold1, ground_truth)
        self.assertEqual(fold2, ground_truth)

    def test_fold_by_key_mutable_zero_value(self):
        # Test for SPARK-9021; uses foldByKey to make a pair RDD that contains
        # lists of all values for each key in the original RDD

        tuples = [(i, range(i)) for i in range(10)]*2
        # Show that single or multiple partitions work
        data1 = self.sc.parallelize(tuples, 1)
        data2 = self.sc.parallelize(tuples, 2)

        def comboOp(x, y):
            x.extend(y)
            return x

        values1 = data1.foldByKey([], comboOp).collect()
        values2 = data2.foldByKey([], comboOp).collect()
        # Sort lists to ensure clean comparison with ground_truth
        values1.sort()
        values2.sort()

        # list(range(...)) for Python 3.x compatibility
        ground_truth = [(i, list(range(i))*2) for i in range(10)]
        self.assertEqual(values1, ground_truth)
        self.assertEqual(values2, ground_truth)

    def test_aggregate_by_key(self):
        data = self.sc.parallelize([(1, 1), (1, 1), (3, 2), (5, 1), (5, 3)], 2)

        def seqOp(x, y):
            x.add(y)
            return x

        def combOp(x, y):
            x |= y
            return x

        sets = dict(data.aggregateByKey(set(), seqOp, combOp).collect())
        self.assertEqual(3, len(sets))
        self.assertEqual(set([1]), sets[1])
        self.assertEqual(set([2]), sets[3])
        self.assertEqual(set([1, 3]), sets[5])

    def test_itemgetter(self):
        rdd = self.sc.parallelize([range(10)])
        from operator import itemgetter
        self.assertEqual([1], rdd.map(itemgetter(1)).collect())
        self.assertEqual([(2, 3)], rdd.map(itemgetter(2, 3)).collect())

    def test_namedtuple_in_rdd(self):
        from collections import namedtuple
        Person = namedtuple("Person", "id firstName lastName")
        jon = Person(1, "Jon", "Doe")
        jane = Person(2, "Jane", "Doe")
        theDoes = self.sc.parallelize([jon, jane])
        self.assertEqual([jon, jane], theDoes.collect())

    def test_large_broadcast(self):
        N = 10000
        data = [[float(i) for i in range(300)] for i in range(N)]
        bdata = self.sc.broadcast(data)  # 27MB
        m = self.sc.parallelize(range(1), 1).map(lambda x: len(bdata.value)).sum()
        self.assertEqual(N, m)

    def test_unpersist(self):
        N = 1000
        data = [[float(i) for i in range(300)] for i in range(N)]
        bdata = self.sc.broadcast(data)  # 3MB
        bdata.unpersist()
        m = self.sc.parallelize(range(1), 1).map(lambda x: len(bdata.value)).sum()
        self.assertEqual(N, m)
        bdata.destroy(blocking=True)
        try:
            self.sc.parallelize(range(1), 1).map(lambda x: len(bdata.value)).sum()
        except Exception as e:
            pass
        else:
            raise Exception("job should fail after destroy the broadcast")

    def test_multiple_broadcasts(self):
        N = 1 << 21
        b1 = self.sc.broadcast(set(range(N)))  # multiple blocks in JVM
        r = list(range(1 << 15))
        random.shuffle(r)
        s = str(r).encode()
        checksum = hashlib.md5(s).hexdigest()
        b2 = self.sc.broadcast(s)
        r = list(set(self.sc.parallelize(range(10), 10).map(
            lambda x: (len(b1.value), hashlib.md5(b2.value).hexdigest())).collect()))
        self.assertEqual(1, len(r))
        size, csum = r[0]
        self.assertEqual(N, size)
        self.assertEqual(checksum, csum)

        random.shuffle(r)
        s = str(r).encode()
        checksum = hashlib.md5(s).hexdigest()
        b2 = self.sc.broadcast(s)
        r = list(set(self.sc.parallelize(range(10), 10).map(
            lambda x: (len(b1.value), hashlib.md5(b2.value).hexdigest())).collect()))
        self.assertEqual(1, len(r))
        size, csum = r[0]
        self.assertEqual(N, size)
        self.assertEqual(checksum, csum)

    def test_multithread_broadcast_pickle(self):
        import threading

        b1 = self.sc.broadcast(list(range(3)))
        b2 = self.sc.broadcast(list(range(3)))

        def f1():
            return b1.value

        def f2():
            return b2.value

        funcs_num_pickled = {f1: None, f2: None}

        def do_pickle(f, sc):
            command = (f, None, sc.serializer, sc.serializer)
            ser = CloudPickleSerializer()
            ser.dumps(command)

        def process_vars(sc):
            broadcast_vars = list(sc._pickled_broadcast_vars)
            num_pickled = len(broadcast_vars)
            sc._pickled_broadcast_vars.clear()
            return num_pickled

        def run(f, sc):
            do_pickle(f, sc)
            funcs_num_pickled[f] = process_vars(sc)

        # pickle f1, adds b1 to sc._pickled_broadcast_vars in main thread local storage
        do_pickle(f1, self.sc)

        # run all for f2, should only add/count/clear b2 from worker thread local storage
        t = threading.Thread(target=run, args=(f2, self.sc))
        t.start()
        t.join()

        # count number of vars pickled in main thread, only b1 should be counted and cleared
        funcs_num_pickled[f1] = process_vars(self.sc)

        self.assertEqual(funcs_num_pickled[f1], 1)
        self.assertEqual(funcs_num_pickled[f2], 1)
        self.assertEqual(len(list(self.sc._pickled_broadcast_vars)), 0)

    def test_large_closure(self):
        N = 200000
        data = [float(i) for i in xrange(N)]
        rdd = self.sc.parallelize(range(1), 1).map(lambda x: len(data))
        self.assertEqual(N, rdd.first())
        # regression test for SPARK-6886
        self.assertEqual(1, rdd.map(lambda x: (x, 1)).groupByKey().count())

    def test_zip_with_different_serializers(self):
        a = self.sc.parallelize(range(5))
        b = self.sc.parallelize(range(100, 105))
        self.assertEqual(a.zip(b).collect(), [(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)])
        a = a._reserialize(BatchedSerializer(PickleSerializer(), 2))
        b = b._reserialize(MarshalSerializer())
        self.assertEqual(a.zip(b).collect(), [(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)])
        # regression test for SPARK-4841
        path = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        t = self.sc.textFile(path)
        cnt = t.count()
        self.assertEqual(cnt, t.zip(t).count())
        rdd = t.map(str)
        self.assertEqual(cnt, t.zip(rdd).count())
        # regression test for bug in _reserializer()
        self.assertEqual(cnt, t.zip(rdd).count())

    def test_zip_with_different_object_sizes(self):
        # regress test for SPARK-5973
        a = self.sc.parallelize(xrange(10000)).map(lambda i: '*' * i)
        b = self.sc.parallelize(xrange(10000, 20000)).map(lambda i: '*' * i)
        self.assertEqual(10000, a.zip(b).count())

    def test_zip_with_different_number_of_items(self):
        a = self.sc.parallelize(range(5), 2)
        # different number of partitions
        b = self.sc.parallelize(range(100, 106), 3)
        self.assertRaises(ValueError, lambda: a.zip(b))
        with QuietTest(self.sc):
            # different number of batched items in JVM
            b = self.sc.parallelize(range(100, 104), 2)
            self.assertRaises(Exception, lambda: a.zip(b).count())
            # different number of items in one pair
            b = self.sc.parallelize(range(100, 106), 2)
            self.assertRaises(Exception, lambda: a.zip(b).count())
            # same total number of items, but different distributions
            a = self.sc.parallelize([2, 3], 2).flatMap(range)
            b = self.sc.parallelize([3, 2], 2).flatMap(range)
            self.assertEqual(a.count(), b.count())
            self.assertRaises(Exception, lambda: a.zip(b).count())

    def test_count_approx_distinct(self):
        rdd = self.sc.parallelize(xrange(1000))
        self.assertTrue(950 < rdd.countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(float).countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(str).countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(lambda x: (x, -x)).countApproxDistinct(0.03) < 1050)

        rdd = self.sc.parallelize([i % 20 for i in range(1000)], 7)
        self.assertTrue(18 < rdd.countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(float).countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(str).countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(lambda x: (x, -x)).countApproxDistinct() < 22)

        self.assertRaises(ValueError, lambda: rdd.countApproxDistinct(0.00000001))

    def test_histogram(self):
        # empty
        rdd = self.sc.parallelize([])
        self.assertEqual([0], rdd.histogram([0, 10])[1])
        self.assertEqual([0, 0], rdd.histogram([0, 4, 10])[1])
        self.assertRaises(ValueError, lambda: rdd.histogram(1))

        # out of range
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEqual([0], rdd.histogram([0, 10])[1])
        self.assertEqual([0, 0], rdd.histogram((0, 4, 10))[1])

        # in range with one bucket
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEqual([4], rdd.histogram([0, 10])[1])
        self.assertEqual([3, 1], rdd.histogram([0, 4, 10])[1])

        # in range with one bucket exact match
        self.assertEqual([4], rdd.histogram([1, 4])[1])

        # out of range with two buckets
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEqual([0, 0], rdd.histogram([0, 5, 10])[1])

        # out of range with two uneven buckets
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEqual([0, 0], rdd.histogram([0, 4, 10])[1])

        # in range with two buckets
        rdd = self.sc.parallelize([1, 2, 3, 5, 6])
        self.assertEqual([3, 2], rdd.histogram([0, 5, 10])[1])

        # in range with two bucket and None
        rdd = self.sc.parallelize([1, 2, 3, 5, 6, None, float('nan')])
        self.assertEqual([3, 2], rdd.histogram([0, 5, 10])[1])

        # in range with two uneven buckets
        rdd = self.sc.parallelize([1, 2, 3, 5, 6])
        self.assertEqual([3, 2], rdd.histogram([0, 5, 11])[1])

        # mixed range with two uneven buckets
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.0, 11.01])
        self.assertEqual([4, 3], rdd.histogram([0, 5, 11])[1])

        # mixed range with four uneven buckets
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0, 200.0, 200.1])
        self.assertEqual([4, 2, 1, 3], rdd.histogram([0.0, 5.0, 11.0, 12.0, 200.0])[1])

        # mixed range with uneven buckets and NaN
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0,
                                   199.0, 200.0, 200.1, None, float('nan')])
        self.assertEqual([4, 2, 1, 3], rdd.histogram([0.0, 5.0, 11.0, 12.0, 200.0])[1])

        # out of range with infinite buckets
        rdd = self.sc.parallelize([10.01, -0.01, float('nan'), float("inf")])
        self.assertEqual([1, 2], rdd.histogram([float('-inf'), 0, float('inf')])[1])

        # invalid buckets
        self.assertRaises(ValueError, lambda: rdd.histogram([]))
        self.assertRaises(ValueError, lambda: rdd.histogram([1]))
        self.assertRaises(ValueError, lambda: rdd.histogram(0))
        self.assertRaises(TypeError, lambda: rdd.histogram({}))

        # without buckets
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEqual(([1, 4], [4]), rdd.histogram(1))

        # without buckets single element
        rdd = self.sc.parallelize([1])
        self.assertEqual(([1, 1], [1]), rdd.histogram(1))

        # without bucket no range
        rdd = self.sc.parallelize([1] * 4)
        self.assertEqual(([1, 1], [4]), rdd.histogram(1))

        # without buckets basic two
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEqual(([1, 2.5, 4], [2, 2]), rdd.histogram(2))

        # without buckets with more requested than elements
        rdd = self.sc.parallelize([1, 2])
        buckets = [1 + 0.2 * i for i in range(6)]
        hist = [1, 0, 0, 0, 1]
        self.assertEqual((buckets, hist), rdd.histogram(5))

        # invalid RDDs
        rdd = self.sc.parallelize([1, float('inf')])
        self.assertRaises(ValueError, lambda: rdd.histogram(2))
        rdd = self.sc.parallelize([float('nan')])
        self.assertRaises(ValueError, lambda: rdd.histogram(2))

        # string
        rdd = self.sc.parallelize(["ab", "ac", "b", "bd", "ef"], 2)
        self.assertEqual([2, 2], rdd.histogram(["a", "b", "c"])[1])
        self.assertEqual((["ab", "ef"], [5]), rdd.histogram(1))
        self.assertRaises(TypeError, lambda: rdd.histogram(2))

    def test_repartitionAndSortWithinPartitions_asc(self):
        rdd = self.sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)], 2)

        repartitioned = rdd.repartitionAndSortWithinPartitions(2, lambda key: key % 2, True)
        partitions = repartitioned.glom().collect()
        self.assertEqual(partitions[0], [(0, 5), (0, 8), (2, 6)])
        self.assertEqual(partitions[1], [(1, 3), (3, 8), (3, 8)])

    def test_repartitionAndSortWithinPartitions_desc(self):
        rdd = self.sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)], 2)

        repartitioned = rdd.repartitionAndSortWithinPartitions(2, lambda key: key % 2, False)
        partitions = repartitioned.glom().collect()
        self.assertEqual(partitions[0], [(2, 6), (0, 5), (0, 8)])
        self.assertEqual(partitions[1], [(3, 8), (3, 8), (1, 3)])

    def test_repartition_no_skewed(self):
        num_partitions = 20
        a = self.sc.parallelize(range(int(1000)), 2)
        l = a.repartition(num_partitions).glom().map(len).collect()
        zeros = len([x for x in l if x == 0])
        self.assertTrue(zeros == 0)
        l = a.coalesce(num_partitions, True).glom().map(len).collect()
        zeros = len([x for x in l if x == 0])
        self.assertTrue(zeros == 0)

    def test_repartition_on_textfile(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello/hello.txt")
        rdd = self.sc.textFile(path)
        result = rdd.repartition(1).collect()
        self.assertEqual(u"Hello World!", result[0])

    def test_distinct(self):
        rdd = self.sc.parallelize((1, 2, 3)*10, 10)
        self.assertEqual(rdd.getNumPartitions(), 10)
        self.assertEqual(rdd.distinct().count(), 3)
        result = rdd.distinct(5)
        self.assertEqual(result.getNumPartitions(), 5)
        self.assertEqual(result.count(), 3)

    def test_external_group_by_key(self):
        self.sc._conf.set("spark.python.worker.memory", "1m")
        N = 2000001
        kv = self.sc.parallelize(xrange(N)).map(lambda x: (x % 3, x))
        gkv = kv.groupByKey().cache()
        self.assertEqual(3, gkv.count())
        filtered = gkv.filter(lambda kv: kv[0] == 1)
        self.assertEqual(1, filtered.count())
        self.assertEqual([(1, N // 3)], filtered.mapValues(len).collect())
        self.assertEqual([(N // 3, N // 3)],
                         filtered.values().map(lambda x: (len(x), len(list(x)))).collect())
        result = filtered.collect()[0][1]
        self.assertEqual(N // 3, len(result))
        self.assertTrue(isinstance(result.data, shuffle.ExternalListOfList))

    def test_sort_on_empty_rdd(self):
        self.assertEqual([], self.sc.parallelize(zip([], [])).sortByKey().collect())

    def test_sample(self):
        rdd = self.sc.parallelize(range(0, 100), 4)
        wo = rdd.sample(False, 0.1, 2).collect()
        wo_dup = rdd.sample(False, 0.1, 2).collect()
        self.assertSetEqual(set(wo), set(wo_dup))
        wr = rdd.sample(True, 0.2, 5).collect()
        wr_dup = rdd.sample(True, 0.2, 5).collect()
        self.assertSetEqual(set(wr), set(wr_dup))
        wo_s10 = rdd.sample(False, 0.3, 10).collect()
        wo_s20 = rdd.sample(False, 0.3, 20).collect()
        self.assertNotEqual(set(wo_s10), set(wo_s20))
        wr_s11 = rdd.sample(True, 0.4, 11).collect()
        wr_s21 = rdd.sample(True, 0.4, 21).collect()
        self.assertNotEqual(set(wr_s11), set(wr_s21))

    def test_null_in_rdd(self):
        jrdd = self.sc._jvm.PythonUtils.generateRDDWithNull(self.sc._jsc)
        rdd = RDD(jrdd, self.sc, UTF8Deserializer())
        self.assertEqual([u"a", None, u"b"], rdd.collect())
        rdd = RDD(jrdd, self.sc, NoOpSerializer())
        self.assertEqual([b"a", None, b"b"], rdd.collect())

    def test_multiple_python_java_RDD_conversions(self):
        # Regression test for SPARK-5361
        data = [
            (u'1', {u'director': u'David Lean'}),
            (u'2', {u'director': u'Andrew Dominik'})
        ]
        data_rdd = self.sc.parallelize(data)
        data_java_rdd = data_rdd._to_java_object_rdd()
        data_python_rdd = self.sc._jvm.SerDeUtil.javaToPython(data_java_rdd)
        converted_rdd = RDD(data_python_rdd, self.sc)
        self.assertEqual(2, converted_rdd.count())

        # conversion between python and java RDD threw exceptions
        data_java_rdd = converted_rdd._to_java_object_rdd()
        data_python_rdd = self.sc._jvm.SerDeUtil.javaToPython(data_java_rdd)
        converted_rdd = RDD(data_python_rdd, self.sc)
        self.assertEqual(2, converted_rdd.count())

    # Regression test for SPARK-6294
    def test_take_on_jrdd(self):
        rdd = self.sc.parallelize(xrange(1 << 20)).map(lambda x: str(x))
        rdd._jrdd.first()

    def test_sortByKey_uses_all_partitions_not_only_first_and_last(self):
        # Regression test for SPARK-5969
        seq = [(i * 59 % 101, i) for i in range(101)]  # unsorted sequence
        rdd = self.sc.parallelize(seq)
        for ascending in [True, False]:
            sort = rdd.sortByKey(ascending=ascending, numPartitions=5)
            self.assertEqual(sort.collect(), sorted(seq, reverse=not ascending))
            sizes = sort.glom().map(len).collect()
            for size in sizes:
                self.assertGreater(size, 0)

    def test_pipe_functions(self):
        data = ['1', '2', '3']
        rdd = self.sc.parallelize(data)
        with QuietTest(self.sc):
            self.assertEqual([], rdd.pipe('java').collect())
            self.assertRaises(Py4JJavaError, rdd.pipe('java', checkCode=True).collect)
        result = rdd.pipe('cat').collect()
        result.sort()
        for x, y in zip(data, result):
            self.assertEqual(x, y)
        self.assertRaises(Py4JJavaError, rdd.pipe('grep 4', checkCode=True).collect)
        self.assertEqual([], rdd.pipe('grep 4').collect())

    def test_pipe_unicode(self):
        # Regression test for SPARK-20947
        data = [u'\u6d4b\u8bd5', '1']
        rdd = self.sc.parallelize(data)
        result = rdd.pipe('cat').collect()
        self.assertEqual(data, result)

    def test_stopiteration_in_user_code(self):

        def stopit(*x):
            raise StopIteration()

        seq_rdd = self.sc.parallelize(range(10))
        keyed_rdd = self.sc.parallelize((x % 2, x) for x in range(10))
        msg = "Caught StopIteration thrown from user's code; failing the task"

        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.map(stopit).collect)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.filter(stopit).collect)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.foreach, stopit)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.reduce, stopit)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.fold, 0, stopit)
        self.assertRaisesRegexp(Py4JJavaError, msg, seq_rdd.foreach, stopit)
        self.assertRaisesRegexp(Py4JJavaError, msg,
                                seq_rdd.cartesian(seq_rdd).flatMap(stopit).collect)

        # these methods call the user function both in the driver and in the executor
        # the exception raised is different according to where the StopIteration happens
        # RuntimeError is raised if in the driver
        # Py4JJavaError is raised if in the executor (wraps the RuntimeError raised in the worker)
        self.assertRaisesRegexp((Py4JJavaError, RuntimeError), msg,
                                keyed_rdd.reduceByKeyLocally, stopit)
        self.assertRaisesRegexp((Py4JJavaError, RuntimeError), msg,
                                seq_rdd.aggregate, 0, stopit, lambda *x: 1)
        self.assertRaisesRegexp((Py4JJavaError, RuntimeError), msg,
                                seq_rdd.aggregate, 0, lambda *x: 1, stopit)

    def test_overwritten_global_func(self):
        # Regression test for SPARK-27000
        global global_func
        self.assertEqual(self.sc.parallelize([1]).map(lambda _: global_func()).first(), "Hi")
        global_func = lambda: "Yeah"
        self.assertEqual(self.sc.parallelize([1]).map(lambda _: global_func()).first(), "Yeah")

    def test_to_local_iterator_failure(self):
        # SPARK-27548 toLocalIterator task failure not propagated to Python driver

        def fail(_):
            raise RuntimeError("local iterator error")

        rdd = self.sc.range(10).map(fail)

        with self.assertRaisesRegexp(Exception, "local iterator error"):
            for _ in rdd.toLocalIterator():
                pass

    def test_to_local_iterator_collects_single_partition(self):
        # Test that partitions are not computed until requested by iteration

        def fail_last(x):
            if x == 9:
                raise RuntimeError("This should not be hit")
            return x

        rdd = self.sc.range(12, numSlices=4).map(fail_last)
        it = rdd.toLocalIterator()

        # Only consume first 4 elements from partitions 1 and 2, this should not collect the last
        # partition which would trigger the error
        for i in range(4):
            self.assertEqual(i, next(it))

    def test_multiple_group_jobs(self):
        import threading
        group_a = "job_ids_to_cancel"
        group_b = "job_ids_to_run"

        threads = []
        thread_ids = range(4)
        thread_ids_to_cancel = [i for i in thread_ids if i % 2 == 0]
        thread_ids_to_run = [i for i in thread_ids if i % 2 != 0]

        # A list which records whether job is cancelled.
        # The index of the array is the thread index which job run in.
        is_job_cancelled = [False for _ in thread_ids]

        def run_job(job_group, index):
            """
            Executes a job with the group ``job_group``. Each job waits for 3 seconds
            and then exits.
            """
            try:
                self.sc.parallelize([15]).map(lambda x: time.sleep(x)) \
                    .collectWithJobGroup(job_group, "test rdd collect with setting job group")
                is_job_cancelled[index] = False
            except Exception:
                # Assume that exception means job cancellation.
                is_job_cancelled[index] = True

        # Test if job succeeded when not cancelled.
        run_job(group_a, 0)
        self.assertFalse(is_job_cancelled[0])

        # Run jobs
        for i in thread_ids_to_cancel:
            t = threading.Thread(target=run_job, args=(group_a, i))
            t.start()
            threads.append(t)

        for i in thread_ids_to_run:
            t = threading.Thread(target=run_job, args=(group_b, i))
            t.start()
            threads.append(t)

        # Wait to make sure all jobs are executed.
        time.sleep(3)
        # And then, cancel one job group.
        self.sc.cancelJobGroup(group_a)

        # Wait until all threads launching jobs are finished.
        for t in threads:
            t.join()

        for i in thread_ids_to_cancel:
            self.assertTrue(
                is_job_cancelled[i],
                "Thread {i}: Job in group A was not cancelled.".format(i=i))

        for i in thread_ids_to_run:
            self.assertFalse(
                is_job_cancelled[i],
                "Thread {i}: Job in group B did not succeeded.".format(i=i))


if __name__ == "__main__":
    import unittest
    from pyspark.tests.test_rdd import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

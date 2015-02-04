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

"""
Unit tests for PySpark; additional tests are implemented as doctests in
individual modules.
"""
from array import array
from fileinput import input
from glob import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
import random
import threading
import hashlib

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest


from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.serializers import read_int, BatchedSerializer, MarshalSerializer, PickleSerializer, \
    CloudPickleSerializer, CompressedSerializer, UTF8Deserializer, NoOpSerializer
from pyspark.shuffle import Aggregator, InMemoryMerger, ExternalMerger, ExternalSorter
from pyspark import shuffle
from pyspark.profiler import BasicProfiler

_have_scipy = False
_have_numpy = False
try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy, but that's okay, we'll skip those tests
    pass
try:
    import numpy as np
    _have_numpy = True
except:
    # No NumPy, but that's okay, we'll skip those tests
    pass


SPARK_HOME = os.environ["SPARK_HOME"]


class MergerTests(unittest.TestCase):

    def setUp(self):
        self.N = 1 << 14
        self.l = [i for i in xrange(self.N)]
        self.data = zip(self.l, self.l)
        self.agg = Aggregator(lambda x: [x],
                              lambda x, y: x.append(y) or x,
                              lambda x, y: x.extend(y) or x)

    def test_in_memory(self):
        m = InMemoryMerger(self.agg)
        m.mergeValues(self.data)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

        m = InMemoryMerger(self.agg)
        m.mergeCombiners(map(lambda (x, y): (x, [y]), self.data))
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

    def test_small_dataset(self):
        m = ExternalMerger(self.agg, 1000)
        m.mergeValues(self.data)
        self.assertEqual(m.spills, 0)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

        m = ExternalMerger(self.agg, 1000)
        m.mergeCombiners(map(lambda (x, y): (x, [y]), self.data))
        self.assertEqual(m.spills, 0)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

    def test_medium_dataset(self):
        m = ExternalMerger(self.agg, 10)
        m.mergeValues(self.data)
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

        m = ExternalMerger(self.agg, 10)
        m.mergeCombiners(map(lambda (x, y): (x, [y]), self.data * 3))
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)) * 3)

    def test_huge_dataset(self):
        m = ExternalMerger(self.agg, 10, partitions=3)
        m.mergeCombiners(map(lambda (k, v): (k, [str(v)]), self.data * 10))
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(len(v) for k, v in m._recursive_merged_items(0)),
                         self.N * 10)
        m._cleanup()


class SorterTests(unittest.TestCase):
    def test_in_memory_sort(self):
        l = range(1024)
        random.shuffle(l)
        sorter = ExternalSorter(1024)
        self.assertEquals(sorted(l), list(sorter.sorted(l)))
        self.assertEquals(sorted(l, reverse=True), list(sorter.sorted(l, reverse=True)))
        self.assertEquals(sorted(l, key=lambda x: -x), list(sorter.sorted(l, key=lambda x: -x)))
        self.assertEquals(sorted(l, key=lambda x: -x, reverse=True),
                          list(sorter.sorted(l, key=lambda x: -x, reverse=True)))

    def test_external_sort(self):
        l = range(1024)
        random.shuffle(l)
        sorter = ExternalSorter(1)
        self.assertEquals(sorted(l), list(sorter.sorted(l)))
        self.assertGreater(shuffle.DiskBytesSpilled, 0)
        last = shuffle.DiskBytesSpilled
        self.assertEquals(sorted(l, reverse=True), list(sorter.sorted(l, reverse=True)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)
        last = shuffle.DiskBytesSpilled
        self.assertEquals(sorted(l, key=lambda x: -x), list(sorter.sorted(l, key=lambda x: -x)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)
        last = shuffle.DiskBytesSpilled
        self.assertEquals(sorted(l, key=lambda x: -x, reverse=True),
                          list(sorter.sorted(l, key=lambda x: -x, reverse=True)))
        self.assertGreater(shuffle.DiskBytesSpilled, last)

    def test_external_sort_in_rdd(self):
        conf = SparkConf().set("spark.python.worker.memory", "1m")
        sc = SparkContext(conf=conf)
        l = range(10240)
        random.shuffle(l)
        rdd = sc.parallelize(l, 10)
        self.assertEquals(sorted(l), rdd.sortBy(lambda x: x).collect())
        sc.stop()


class SerializationTestCase(unittest.TestCase):

    def test_namedtuple(self):
        from collections import namedtuple
        from cPickle import dumps, loads
        P = namedtuple("P", "x y")
        p1 = P(1, 3)
        p2 = loads(dumps(p1, 2))
        self.assertEquals(p1, p2)

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
        ser = CloudPickleSerializer()
        out1 = sys.stderr
        out2 = ser.loads(ser.dumps(out1))
        self.assertEquals(out1, out2)

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

        self.assertTrue("exit" in foo.func_code.co_names)
        ser.dumps(foo)

    def test_compressed_serializer(self):
        ser = CompressedSerializer(PickleSerializer())
        from StringIO import StringIO
        io = StringIO()
        ser.dump_stream(["abc", u"123", range(5)], io)
        io.seek(0)
        self.assertEqual(["abc", u"123", range(5)], list(ser.load_stream(io)))
        ser.dump_stream(range(1000), io)
        io.seek(0)
        self.assertEqual(["abc", u"123", range(5)] + range(1000), list(ser.load_stream(io)))


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.sc = SparkContext('local[4]', class_name)

    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path


class ReusedPySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext('local[4]', cls.__name__)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


class CheckpointTests(ReusedPySparkTestCase):

    def setUp(self):
        self.checkpointDir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.checkpointDir.name)
        self.sc.setCheckpointDir(self.checkpointDir.name)

    def tearDown(self):
        shutil.rmtree(self.checkpointDir.name)

    def test_basic_checkpointing(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)
        self.assertEqual("file:" + self.checkpointDir.name,
                         os.path.dirname(os.path.dirname(flatMappedRDD.getCheckpointFile())))

    def test_checkpoint_and_restore(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: [x])

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        flatMappedRDD.count()  # forces a checkpoint to be computed
        time.sleep(1)  # 1 second

        self.assertTrue(flatMappedRDD.getCheckpointFile() is not None)
        recovered = self.sc._checkpointFile(flatMappedRDD.getCheckpointFile(),
                                            flatMappedRDD._jrdd_deserializer)
        self.assertEquals([1, 2, 3, 4], recovered.collect())


class AddFileTests(PySparkTestCase):

    def test_add_py_file(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this job fails due to `userlibrary` not being on the Python path:
        # disable logging in log4j temporarily
        log4j = self.sc._jvm.org.apache.log4j
        old_level = log4j.LogManager.getRootLogger().getLevel()
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

        def func(x):
            from userlibrary import UserClass
            return UserClass().hello()
        self.assertRaises(Exception,
                          self.sc.parallelize(range(2)).map(func).first)
        log4j.LogManager.getRootLogger().setLevel(old_level)

        # Add the file, so the job should now succeed:
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addPyFile(path)
        res = self.sc.parallelize(range(2)).map(func).first()
        self.assertEqual("Hello World!", res)

    def test_add_file_locally(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        self.sc.addFile(path)
        download_path = SparkFiles.get("hello.txt")
        self.assertNotEqual(path, download_path)
        with open(download_path) as test_file:
            self.assertEquals("Hello World!\n", test_file.readline())

    def test_add_py_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlibrary import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addFile(path)
        from userlibrary import UserClass
        self.assertEqual("Hello World!", UserClass().hello())

    def test_add_egg_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlib import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlib-0.1-py2.7.egg")
        self.sc.addPyFile(path)
        from userlib import UserClass
        self.assertEqual("Hello World from inside a package!", UserClass().hello())

    def test_overwrite_system_module(self):
        self.sc.addPyFile(os.path.join(SPARK_HOME, "python/test_support/SimpleHTTPServer.py"))

        import SimpleHTTPServer
        self.assertEqual("My Server", SimpleHTTPServer.__name__)

        def func(x):
            import SimpleHTTPServer
            return SimpleHTTPServer.__name__

        self.assertEqual(["My Server"], self.sc.parallelize(range(1)).map(func).collect())


class RDDTests(ReusedPySparkTestCase):

    def test_id(self):
        rdd = self.sc.parallelize(range(10))
        id = rdd.id()
        self.assertEqual(id, rdd.id())
        rdd2 = rdd.map(str).filter(bool)
        id2 = rdd2.id()
        self.assertEqual(id + 1, id2)
        self.assertEqual(id2, rdd2.id())

    def test_save_as_textfile_with_unicode(self):
        # Regression test for SPARK-970
        x = u"\u00A1Hola, mundo!"
        data = self.sc.parallelize([x])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsTextFile(tempFile.name)
        raw_contents = ''.join(input(glob(tempFile.name + "/part-0000*")))
        self.assertEqual(x, unicode(raw_contents.strip(), "utf-8"))

    def test_save_as_textfile_with_utf8(self):
        x = u"\u00A1Hola, mundo!"
        data = self.sc.parallelize([x.encode("utf-8")])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsTextFile(tempFile.name)
        raw_contents = ''.join(input(glob(tempFile.name + "/part-0000*")))
        self.assertEqual(x, unicode(raw_contents.strip(), "utf-8"))

    def test_transforming_cartesian_result(self):
        # Regression test for SPARK-1034
        rdd1 = self.sc.parallelize([1, 2])
        rdd2 = self.sc.parallelize([3, 4])
        cart = rdd1.cartesian(rdd2)
        result = cart.map(lambda (x, y): x + y).collect()

    def test_transforming_pickle_file(self):
        # Regression test for SPARK-2601
        data = self.sc.parallelize(["Hello", "World!"])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsPickleFile(tempFile.name)
        pickled_file = self.sc.pickleFile(tempFile.name)
        pickled_file.map(lambda x: x).collect()

    def test_cartesian_on_textfile(self):
        # Regression test for
        path = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        a = self.sc.textFile(path)
        result = a.cartesian(a).collect()
        (x, y) = result[0]
        self.assertEqual("Hello World!", x.strip())
        self.assertEqual("Hello World!", y.strip())

    def test_deleting_input_files(self):
        # Regression test for SPARK-1025
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write("Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        self.assertRaises(Exception, lambda: filtered_data.count())

    def test_sampling_default_seed(self):
        # Test for SPARK-3995 (default seed setting)
        data = self.sc.parallelize(range(1000), 1)
        subset = data.takeSample(False, 10)
        self.assertEqual(len(subset), 10)

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
        self.assertEquals([jon, jane], theDoes.collect())

    def test_large_broadcast(self):
        N = 100000
        data = [[float(i) for i in range(300)] for i in range(N)]
        bdata = self.sc.broadcast(data)  # 270MB
        m = self.sc.parallelize(range(1), 1).map(lambda x: len(bdata.value)).sum()
        self.assertEquals(N, m)

    def test_multiple_broadcasts(self):
        N = 1 << 21
        b1 = self.sc.broadcast(set(range(N)))  # multiple blocks in JVM
        r = range(1 << 15)
        random.shuffle(r)
        s = str(r)
        checksum = hashlib.md5(s).hexdigest()
        b2 = self.sc.broadcast(s)
        r = list(set(self.sc.parallelize(range(10), 10).map(
            lambda x: (len(b1.value), hashlib.md5(b2.value).hexdigest())).collect()))
        self.assertEqual(1, len(r))
        size, csum = r[0]
        self.assertEqual(N, size)
        self.assertEqual(checksum, csum)

        random.shuffle(r)
        s = str(r)
        checksum = hashlib.md5(s).hexdigest()
        b2 = self.sc.broadcast(s)
        r = list(set(self.sc.parallelize(range(10), 10).map(
            lambda x: (len(b1.value), hashlib.md5(b2.value).hexdigest())).collect()))
        self.assertEqual(1, len(r))
        size, csum = r[0]
        self.assertEqual(N, size)
        self.assertEqual(checksum, csum)

    def test_large_closure(self):
        N = 1000000
        data = [float(i) for i in xrange(N)]
        rdd = self.sc.parallelize(range(1), 1).map(lambda x: len(data))
        self.assertEquals(N, rdd.first())
        self.assertTrue(rdd._broadcast is not None)
        rdd = self.sc.parallelize(range(1), 1).map(lambda x: 1)
        self.assertEqual(1, rdd.first())
        self.assertTrue(rdd._broadcast is None)

    def test_zip_with_different_serializers(self):
        a = self.sc.parallelize(range(5))
        b = self.sc.parallelize(range(100, 105))
        self.assertEqual(a.zip(b).collect(), [(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)])
        a = a._reserialize(BatchedSerializer(PickleSerializer(), 2))
        b = b._reserialize(MarshalSerializer())
        self.assertEqual(a.zip(b).collect(), [(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)])
        # regression test for SPARK-4841
        path = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        t = self.sc.textFile(path)
        cnt = t.count()
        self.assertEqual(cnt, t.zip(t).count())
        rdd = t.map(str)
        self.assertEqual(cnt, t.zip(rdd).count())
        # regression test for bug in _reserializer()
        self.assertEqual(cnt, t.zip(rdd).count())

    def test_zip_with_different_number_of_items(self):
        a = self.sc.parallelize(range(5), 2)
        # different number of partitions
        b = self.sc.parallelize(range(100, 106), 3)
        self.assertRaises(ValueError, lambda: a.zip(b))
        # different number of batched items in JVM
        b = self.sc.parallelize(range(100, 104), 2)
        self.assertRaises(Exception, lambda: a.zip(b).count())
        # different number of items in one pair
        b = self.sc.parallelize(range(100, 106), 2)
        self.assertRaises(Exception, lambda: a.zip(b).count())
        # same total number of items, but different distributions
        a = self.sc.parallelize([2, 3], 2).flatMap(range)
        b = self.sc.parallelize([3, 2], 2).flatMap(range)
        self.assertEquals(a.count(), b.count())
        self.assertRaises(Exception, lambda: a.zip(b).count())

    def test_count_approx_distinct(self):
        rdd = self.sc.parallelize(range(1000))
        self.assertTrue(950 < rdd.countApproxDistinct(0.04) < 1050)
        self.assertTrue(950 < rdd.map(float).countApproxDistinct(0.04) < 1050)
        self.assertTrue(950 < rdd.map(str).countApproxDistinct(0.04) < 1050)
        self.assertTrue(950 < rdd.map(lambda x: (x, -x)).countApproxDistinct(0.04) < 1050)

        rdd = self.sc.parallelize([i % 20 for i in range(1000)], 7)
        self.assertTrue(18 < rdd.countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(float).countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(str).countApproxDistinct() < 22)
        self.assertTrue(18 < rdd.map(lambda x: (x, -x)).countApproxDistinct() < 22)

        self.assertRaises(ValueError, lambda: rdd.countApproxDistinct(0.00000001))
        self.assertRaises(ValueError, lambda: rdd.countApproxDistinct(0.5))

    def test_histogram(self):
        # empty
        rdd = self.sc.parallelize([])
        self.assertEquals([0], rdd.histogram([0, 10])[1])
        self.assertEquals([0, 0], rdd.histogram([0, 4, 10])[1])
        self.assertRaises(ValueError, lambda: rdd.histogram(1))

        # out of range
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEquals([0], rdd.histogram([0, 10])[1])
        self.assertEquals([0, 0], rdd.histogram((0, 4, 10))[1])

        # in range with one bucket
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEquals([4], rdd.histogram([0, 10])[1])
        self.assertEquals([3, 1], rdd.histogram([0, 4, 10])[1])

        # in range with one bucket exact match
        self.assertEquals([4], rdd.histogram([1, 4])[1])

        # out of range with two buckets
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEquals([0, 0], rdd.histogram([0, 5, 10])[1])

        # out of range with two uneven buckets
        rdd = self.sc.parallelize([10.01, -0.01])
        self.assertEquals([0, 0], rdd.histogram([0, 4, 10])[1])

        # in range with two buckets
        rdd = self.sc.parallelize([1, 2, 3, 5, 6])
        self.assertEquals([3, 2], rdd.histogram([0, 5, 10])[1])

        # in range with two bucket and None
        rdd = self.sc.parallelize([1, 2, 3, 5, 6, None, float('nan')])
        self.assertEquals([3, 2], rdd.histogram([0, 5, 10])[1])

        # in range with two uneven buckets
        rdd = self.sc.parallelize([1, 2, 3, 5, 6])
        self.assertEquals([3, 2], rdd.histogram([0, 5, 11])[1])

        # mixed range with two uneven buckets
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.0, 11.01])
        self.assertEquals([4, 3], rdd.histogram([0, 5, 11])[1])

        # mixed range with four uneven buckets
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0, 199.0, 200.0, 200.1])
        self.assertEquals([4, 2, 1, 3], rdd.histogram([0.0, 5.0, 11.0, 12.0, 200.0])[1])

        # mixed range with uneven buckets and NaN
        rdd = self.sc.parallelize([-0.01, 0.0, 1, 2, 3, 5, 6, 11.01, 12.0,
                                   199.0, 200.0, 200.1, None, float('nan')])
        self.assertEquals([4, 2, 1, 3], rdd.histogram([0.0, 5.0, 11.0, 12.0, 200.0])[1])

        # out of range with infinite buckets
        rdd = self.sc.parallelize([10.01, -0.01, float('nan'), float("inf")])
        self.assertEquals([1, 2], rdd.histogram([float('-inf'), 0, float('inf')])[1])

        # invalid buckets
        self.assertRaises(ValueError, lambda: rdd.histogram([]))
        self.assertRaises(ValueError, lambda: rdd.histogram([1]))
        self.assertRaises(ValueError, lambda: rdd.histogram(0))
        self.assertRaises(TypeError, lambda: rdd.histogram({}))

        # without buckets
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEquals(([1, 4], [4]), rdd.histogram(1))

        # without buckets single element
        rdd = self.sc.parallelize([1])
        self.assertEquals(([1, 1], [1]), rdd.histogram(1))

        # without bucket no range
        rdd = self.sc.parallelize([1] * 4)
        self.assertEquals(([1, 1], [4]), rdd.histogram(1))

        # without buckets basic two
        rdd = self.sc.parallelize(range(1, 5))
        self.assertEquals(([1, 2.5, 4], [2, 2]), rdd.histogram(2))

        # without buckets with more requested than elements
        rdd = self.sc.parallelize([1, 2])
        buckets = [1 + 0.2 * i for i in range(6)]
        hist = [1, 0, 0, 0, 1]
        self.assertEquals((buckets, hist), rdd.histogram(5))

        # invalid RDDs
        rdd = self.sc.parallelize([1, float('inf')])
        self.assertRaises(ValueError, lambda: rdd.histogram(2))
        rdd = self.sc.parallelize([float('nan')])
        self.assertRaises(ValueError, lambda: rdd.histogram(2))

        # string
        rdd = self.sc.parallelize(["ab", "ac", "b", "bd", "ef"], 2)
        self.assertEquals([2, 2], rdd.histogram(["a", "b", "c"])[1])
        self.assertEquals((["ab", "ef"], [5]), rdd.histogram(1))
        self.assertRaises(TypeError, lambda: rdd.histogram(2))

        # mixed RDD
        rdd = self.sc.parallelize([1, 4, "ab", "ac", "b"], 2)
        self.assertEquals([1, 1], rdd.histogram([0, 4, 10])[1])
        self.assertEquals([2, 1], rdd.histogram(["a", "b", "c"])[1])
        self.assertEquals(([1, "b"], [5]), rdd.histogram(1))
        self.assertRaises(TypeError, lambda: rdd.histogram(2))

    def test_repartitionAndSortWithinPartitions(self):
        rdd = self.sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)], 2)

        repartitioned = rdd.repartitionAndSortWithinPartitions(2, lambda key: key % 2)
        partitions = repartitioned.glom().collect()
        self.assertEquals(partitions[0], [(0, 5), (0, 8), (2, 6)])
        self.assertEquals(partitions[1], [(1, 3), (3, 8), (3, 8)])

    def test_distinct(self):
        rdd = self.sc.parallelize((1, 2, 3)*10, 10)
        self.assertEquals(rdd.getNumPartitions(), 10)
        self.assertEquals(rdd.distinct().count(), 3)
        result = rdd.distinct(5)
        self.assertEquals(result.getNumPartitions(), 5)
        self.assertEquals(result.count(), 3)

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
        self.assertEqual(["a", None, "b"], rdd.collect())

    def test_multiple_python_java_RDD_conversions(self):
        # Regression test for SPARK-5361
        data = [
            (u'1', {u'director': u'David Lean'}),
            (u'2', {u'director': u'Andrew Dominik'})
        ]
        from pyspark.rdd import RDD
        data_rdd = self.sc.parallelize(data)
        data_java_rdd = data_rdd._to_java_object_rdd()
        data_python_rdd = self.sc._jvm.SerDe.javaToPython(data_java_rdd)
        converted_rdd = RDD(data_python_rdd, self.sc)
        self.assertEqual(2, converted_rdd.count())

        # conversion between python and java RDD threw exceptions
        data_java_rdd = converted_rdd._to_java_object_rdd()
        data_python_rdd = self.sc._jvm.SerDe.javaToPython(data_java_rdd)
        converted_rdd = RDD(data_python_rdd, self.sc)
        self.assertEqual(2, converted_rdd.count())


class ProfilerTests(PySparkTestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.profile", "true")
        self.sc = SparkContext('local[4]', class_name, conf=conf)

    def test_profiler(self):
        self.do_computation()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(1, len(profilers))
        id, profiler, _ = profilers[0]
        stats = profiler.stats()
        self.assertTrue(stats is not None)
        width, stat_list = stats.get_print_list([])
        func_names = [func_name for fname, n, func_name in stat_list]
        self.assertTrue("heavy_foo" in func_names)

        self.sc.show_profiles()
        d = tempfile.gettempdir()
        self.sc.dump_profiles(d)
        self.assertTrue("rdd_%d.pstats" % id in os.listdir(d))

    def test_custom_profiler(self):
        class TestCustomProfiler(BasicProfiler):
            def show(self, id):
                self.result = "Custom formatting"

        self.sc.profiler_collector.profiler_cls = TestCustomProfiler

        self.do_computation()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(1, len(profilers))
        _, profiler, _ = profilers[0]
        self.assertTrue(isinstance(profiler, TestCustomProfiler))

        self.sc.show_profiles()
        self.assertEqual("Custom formatting", profiler.result)

    def do_computation(self):
        def heavy_foo(x):
            for i in range(1 << 20):
                x = 1

        rdd = self.sc.parallelize(range(100))
        rdd.foreach(heavy_foo)


class InputFormatTests(ReusedPySparkTestCase):

    @classmethod
    def setUpClass(cls):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.sc._jvm.WriteInputFormatTestDataGenerator.generateData(cls.tempdir.name, cls.sc._jsc)

    @classmethod
    def tearDownClass(cls):
        ReusedPySparkTestCase.tearDownClass()
        shutil.rmtree(cls.tempdir.name)

    def test_sequencefiles(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfint/",
                                           "org.apache.hadoop.io.IntWritable",
                                           "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        doubles = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfdouble/",
                                              "org.apache.hadoop.io.DoubleWritable",
                                              "org.apache.hadoop.io.Text").collect())
        ed = [(1.0, u'aa'), (1.0, u'aa'), (2.0, u'aa'), (2.0, u'bb'), (2.0, u'bb'), (3.0, u'cc')]
        self.assertEqual(doubles, ed)

        bytes = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfbytes/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BytesWritable").collect())
        ebs = [(1, bytearray('aa', 'utf-8')),
               (1, bytearray('aa', 'utf-8')),
               (2, bytearray('aa', 'utf-8')),
               (2, bytearray('bb', 'utf-8')),
               (2, bytearray('bb', 'utf-8')),
               (3, bytearray('cc', 'utf-8'))]
        self.assertEqual(bytes, ebs)

        text = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sftext/",
                                           "org.apache.hadoop.io.Text",
                                           "org.apache.hadoop.io.Text").collect())
        et = [(u'1', u'aa'),
              (u'1', u'aa'),
              (u'2', u'aa'),
              (u'2', u'bb'),
              (u'2', u'bb'),
              (u'3', u'cc')]
        self.assertEqual(text, et)

        bools = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfbool/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BooleanWritable").collect())
        eb = [(1, False), (1, True), (2, False), (2, False), (2, True), (3, True)]
        self.assertEqual(bools, eb)

        nulls = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfnull/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BooleanWritable").collect())
        en = [(1, None), (1, None), (2, None), (2, None), (2, None), (3, None)]
        self.assertEqual(nulls, en)

        maps = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfmap/",
                                           "org.apache.hadoop.io.IntWritable",
                                           "org.apache.hadoop.io.MapWritable").collect())
        em = [(1, {}),
              (1, {3.0: u'bb'}),
              (2, {1.0: u'aa'}),
              (2, {1.0: u'cc'}),
              (3, {2.0: u'dd'})]
        self.assertEqual(maps, em)

        # arrays get pickled to tuples by default
        tuples = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfarray/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable").collect())
        et = [(1, ()),
              (2, (3.0, 4.0, 5.0)),
              (3, (4.0, 5.0, 6.0))]
        self.assertEqual(tuples, et)

        # with custom converters, primitive arrays can stay as arrays
        arrays = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfarray/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter").collect())
        ea = [(1, array('d')),
              (2, array('d', [3.0, 4.0, 5.0])),
              (3, array('d', [4.0, 5.0, 6.0]))]
        self.assertEqual(arrays, ea)

        clazz = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfclass/",
                                            "org.apache.hadoop.io.Text",
                                            "org.apache.spark.api.python.TestWritable").collect())
        cname = u'org.apache.spark.api.python.TestWritable'
        ec = [(u'1', {u'__class__': cname, u'double': 1.0, u'int': 1, u'str': u'test1'}),
              (u'2', {u'__class__': cname, u'double': 2.3, u'int': 2, u'str': u'test2'}),
              (u'3', {u'__class__': cname, u'double': 3.1, u'int': 3, u'str': u'test3'}),
              (u'4', {u'__class__': cname, u'double': 4.2, u'int': 4, u'str': u'test4'}),
              (u'5', {u'__class__': cname, u'double': 5.5, u'int': 5, u'str': u'test56'})]
        self.assertEqual(clazz, ec)

        unbatched_clazz = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfclass/",
                                                      "org.apache.hadoop.io.Text",
                                                      "org.apache.spark.api.python.TestWritable",
                                                      ).collect())
        self.assertEqual(unbatched_clazz, ec)

    def test_oldhadoop(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.hadoopFile(basepath + "/sftestdata/sfint/",
                                         "org.apache.hadoop.mapred.SequenceFileInputFormat",
                                         "org.apache.hadoop.io.IntWritable",
                                         "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        hellopath = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        oldconf = {"mapred.input.dir": hellopath}
        hello = self.sc.hadoopRDD("org.apache.hadoop.mapred.TextInputFormat",
                                  "org.apache.hadoop.io.LongWritable",
                                  "org.apache.hadoop.io.Text",
                                  conf=oldconf).collect()
        result = [(0, u'Hello World!')]
        self.assertEqual(hello, result)

    def test_newhadoop(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        hellopath = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        newconf = {"mapred.input.dir": hellopath}
        hello = self.sc.newAPIHadoopRDD("org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                        "org.apache.hadoop.io.LongWritable",
                                        "org.apache.hadoop.io.Text",
                                        conf=newconf).collect()
        result = [(0, u'Hello World!')]
        self.assertEqual(hello, result)

    def test_newolderror(self):
        basepath = self.tempdir.name
        self.assertRaises(Exception, lambda: self.sc.hadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

        self.assertRaises(Exception, lambda: self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

    def test_bad_inputs(self):
        basepath = self.tempdir.name
        self.assertRaises(Exception, lambda: self.sc.sequenceFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.io.NotValidWritable",
            "org.apache.hadoop.io.Text"))
        self.assertRaises(Exception, lambda: self.sc.hadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapred.NotValidInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))
        self.assertRaises(Exception, lambda: self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.NotValidInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

    def test_converters(self):
        # use of custom converters
        basepath = self.tempdir.name
        maps = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfmap/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable",
            keyConverter="org.apache.spark.api.python.TestInputKeyConverter",
            valueConverter="org.apache.spark.api.python.TestInputValueConverter").collect())
        em = [(u'\x01', []),
              (u'\x01', [3.0]),
              (u'\x02', [1.0]),
              (u'\x02', [1.0]),
              (u'\x03', [2.0])]
        self.assertEqual(maps, em)

    def test_binary_files(self):
        path = os.path.join(self.tempdir.name, "binaryfiles")
        os.mkdir(path)
        data = "short binary data"
        with open(os.path.join(path, "part-0000"), 'w') as f:
            f.write(data)
        [(p, d)] = self.sc.binaryFiles(path).collect()
        self.assertTrue(p.endswith("part-0000"))
        self.assertEqual(d, data)

    def test_binary_records(self):
        path = os.path.join(self.tempdir.name, "binaryrecords")
        os.mkdir(path)
        with open(os.path.join(path, "part-0000"), 'w') as f:
            for i in range(100):
                f.write('%04d' % i)
        result = self.sc.binaryRecords(path, 4).map(int).collect()
        self.assertEqual(range(100), result)


class OutputFormatTests(ReusedPySparkTestCase):

    def setUp(self):
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)

    def tearDown(self):
        shutil.rmtree(self.tempdir.name, ignore_errors=True)

    def test_sequencefiles(self):
        basepath = self.tempdir.name
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.sc.parallelize(ei).saveAsSequenceFile(basepath + "/sfint/")
        ints = sorted(self.sc.sequenceFile(basepath + "/sfint/").collect())
        self.assertEqual(ints, ei)

        ed = [(1.0, u'aa'), (1.0, u'aa'), (2.0, u'aa'), (2.0, u'bb'), (2.0, u'bb'), (3.0, u'cc')]
        self.sc.parallelize(ed).saveAsSequenceFile(basepath + "/sfdouble/")
        doubles = sorted(self.sc.sequenceFile(basepath + "/sfdouble/").collect())
        self.assertEqual(doubles, ed)

        ebs = [(1, bytearray(b'\x00\x07spam\x08')), (2, bytearray(b'\x00\x07spam\x08'))]
        self.sc.parallelize(ebs).saveAsSequenceFile(basepath + "/sfbytes/")
        bytes = sorted(self.sc.sequenceFile(basepath + "/sfbytes/").collect())
        self.assertEqual(bytes, ebs)

        et = [(u'1', u'aa'),
              (u'2', u'bb'),
              (u'3', u'cc')]
        self.sc.parallelize(et).saveAsSequenceFile(basepath + "/sftext/")
        text = sorted(self.sc.sequenceFile(basepath + "/sftext/").collect())
        self.assertEqual(text, et)

        eb = [(1, False), (1, True), (2, False), (2, False), (2, True), (3, True)]
        self.sc.parallelize(eb).saveAsSequenceFile(basepath + "/sfbool/")
        bools = sorted(self.sc.sequenceFile(basepath + "/sfbool/").collect())
        self.assertEqual(bools, eb)

        en = [(1, None), (1, None), (2, None), (2, None), (2, None), (3, None)]
        self.sc.parallelize(en).saveAsSequenceFile(basepath + "/sfnull/")
        nulls = sorted(self.sc.sequenceFile(basepath + "/sfnull/").collect())
        self.assertEqual(nulls, en)

        em = [(1, {}),
              (1, {3.0: u'bb'}),
              (2, {1.0: u'aa'}),
              (2, {1.0: u'cc'}),
              (3, {2.0: u'dd'})]
        self.sc.parallelize(em).saveAsSequenceFile(basepath + "/sfmap/")
        maps = sorted(self.sc.sequenceFile(basepath + "/sfmap/").collect())
        self.assertEqual(maps, em)

    def test_oldhadoop(self):
        basepath = self.tempdir.name
        dict_data = [(1, {}),
                     (1, {"row1": 1.0}),
                     (2, {"row2": 2.0})]
        self.sc.parallelize(dict_data).saveAsHadoopFile(
            basepath + "/oldhadoop/",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable")
        result = sorted(self.sc.hadoopFile(
            basepath + "/oldhadoop/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable").collect())
        self.assertEqual(result, dict_data)

        conf = {
            "mapred.output.format.class": "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.value.class": "org.apache.hadoop.io.MapWritable",
            "mapred.output.dir": basepath + "/olddataset/"
        }
        self.sc.parallelize(dict_data).saveAsHadoopDataset(conf)
        input_conf = {"mapred.input.dir": basepath + "/olddataset/"}
        old_dataset = sorted(self.sc.hadoopRDD(
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable",
            conf=input_conf).collect())
        self.assertEqual(old_dataset, dict_data)

    def test_newhadoop(self):
        basepath = self.tempdir.name
        data = [(1, ""),
                (1, "a"),
                (2, "bcdf")]
        self.sc.parallelize(data).saveAsNewAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text")
        result = sorted(self.sc.newAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text").collect())
        self.assertEqual(result, data)

        conf = {
            "mapreduce.outputformat.class":
                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.value.class": "org.apache.hadoop.io.Text",
            "mapred.output.dir": basepath + "/newdataset/"
        }
        self.sc.parallelize(data).saveAsNewAPIHadoopDataset(conf)
        input_conf = {"mapred.input.dir": basepath + "/newdataset/"}
        new_dataset = sorted(self.sc.newAPIHadoopRDD(
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text",
            conf=input_conf).collect())
        self.assertEqual(new_dataset, data)

    def test_newhadoop_with_array(self):
        basepath = self.tempdir.name
        # use custom ArrayWritable types and converters to handle arrays
        array_data = [(1, array('d')),
                      (1, array('d', [1.0, 2.0, 3.0])),
                      (2, array('d', [3.0, 4.0, 5.0]))]
        self.sc.parallelize(array_data).saveAsNewAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.DoubleArrayToWritableConverter")
        result = sorted(self.sc.newAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter").collect())
        self.assertEqual(result, array_data)

        conf = {
            "mapreduce.outputformat.class":
                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.value.class": "org.apache.spark.api.python.DoubleArrayWritable",
            "mapred.output.dir": basepath + "/newdataset/"
        }
        self.sc.parallelize(array_data).saveAsNewAPIHadoopDataset(
            conf,
            valueConverter="org.apache.spark.api.python.DoubleArrayToWritableConverter")
        input_conf = {"mapred.input.dir": basepath + "/newdataset/"}
        new_dataset = sorted(self.sc.newAPIHadoopRDD(
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter",
            conf=input_conf).collect())
        self.assertEqual(new_dataset, array_data)

    def test_newolderror(self):
        basepath = self.tempdir.name
        rdd = self.sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
        self.assertRaises(Exception, lambda: rdd.saveAsHadoopFile(
            basepath + "/newolderror/saveAsHadoopFile/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"))
        self.assertRaises(Exception, lambda: rdd.saveAsNewAPIHadoopFile(
            basepath + "/newolderror/saveAsNewAPIHadoopFile/",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat"))

    def test_bad_inputs(self):
        basepath = self.tempdir.name
        rdd = self.sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
        self.assertRaises(Exception, lambda: rdd.saveAsHadoopFile(
            basepath + "/badinputs/saveAsHadoopFile/",
            "org.apache.hadoop.mapred.NotValidOutputFormat"))
        self.assertRaises(Exception, lambda: rdd.saveAsNewAPIHadoopFile(
            basepath + "/badinputs/saveAsNewAPIHadoopFile/",
            "org.apache.hadoop.mapreduce.lib.output.NotValidOutputFormat"))

    def test_converters(self):
        # use of custom converters
        basepath = self.tempdir.name
        data = [(1, {3.0: u'bb'}),
                (2, {1.0: u'aa'}),
                (3, {2.0: u'dd'})]
        self.sc.parallelize(data).saveAsNewAPIHadoopFile(
            basepath + "/converters/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            keyConverter="org.apache.spark.api.python.TestOutputKeyConverter",
            valueConverter="org.apache.spark.api.python.TestOutputValueConverter")
        converted = sorted(self.sc.sequenceFile(basepath + "/converters/").collect())
        expected = [(u'1', 3.0),
                    (u'2', 1.0),
                    (u'3', 2.0)]
        self.assertEqual(converted, expected)

    def test_reserialization(self):
        basepath = self.tempdir.name
        x = range(1, 5)
        y = range(1001, 1005)
        data = zip(x, y)
        rdd = self.sc.parallelize(x).zip(self.sc.parallelize(y))
        rdd.saveAsSequenceFile(basepath + "/reserialize/sequence")
        result1 = sorted(self.sc.sequenceFile(basepath + "/reserialize/sequence").collect())
        self.assertEqual(result1, data)

        rdd.saveAsHadoopFile(
            basepath + "/reserialize/hadoop",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat")
        result2 = sorted(self.sc.sequenceFile(basepath + "/reserialize/hadoop").collect())
        self.assertEqual(result2, data)

        rdd.saveAsNewAPIHadoopFile(
            basepath + "/reserialize/newhadoop",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
        result3 = sorted(self.sc.sequenceFile(basepath + "/reserialize/newhadoop").collect())
        self.assertEqual(result3, data)

        conf4 = {
            "mapred.output.format.class": "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.value.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.dir": basepath + "/reserialize/dataset"}
        rdd.saveAsHadoopDataset(conf4)
        result4 = sorted(self.sc.sequenceFile(basepath + "/reserialize/dataset").collect())
        self.assertEqual(result4, data)

        conf5 = {"mapreduce.outputformat.class":
                 "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                 "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
                 "mapred.output.value.class": "org.apache.hadoop.io.IntWritable",
                 "mapred.output.dir": basepath + "/reserialize/newdataset"}
        rdd.saveAsNewAPIHadoopDataset(conf5)
        result5 = sorted(self.sc.sequenceFile(basepath + "/reserialize/newdataset").collect())
        self.assertEqual(result5, data)

    def test_malformed_RDD(self):
        basepath = self.tempdir.name
        # non-batch-serialized RDD[[(K, V)]] should be rejected
        data = [[(1, "a")], [(2, "aa")], [(3, "aaa")]]
        rdd = self.sc.parallelize(data, len(data))
        self.assertRaises(Exception, lambda: rdd.saveAsSequenceFile(
            basepath + "/malformed/sequence"))


class DaemonTests(unittest.TestCase):
    def connect(self, port):
        from socket import socket, AF_INET, SOCK_STREAM
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(('127.0.0.1', port))
        # send a split index of -1 to shutdown the worker
        sock.send("\xFF\xFF\xFF\xFF")
        sock.close()
        return True

    def do_termination_test(self, terminator):
        from subprocess import Popen, PIPE
        from errno import ECONNREFUSED

        # start daemon
        daemon_path = os.path.join(os.path.dirname(__file__), "daemon.py")
        daemon = Popen([sys.executable, daemon_path], stdin=PIPE, stdout=PIPE)

        # read the port number
        port = read_int(daemon.stdout)

        # daemon should accept connections
        self.assertTrue(self.connect(port))

        # request shutdown
        terminator(daemon)
        time.sleep(1)

        # daemon should no longer accept connections
        try:
            self.connect(port)
        except EnvironmentError as exception:
            self.assertEqual(exception.errno, ECONNREFUSED)
        else:
            self.fail("Expected EnvironmentError to be raised")

    def test_termination_stdin(self):
        """Ensure that daemon and workers terminate when stdin is closed."""
        self.do_termination_test(lambda daemon: daemon.stdin.close())

    def test_termination_sigterm(self):
        """Ensure that daemon and workers terminate on SIGTERM."""
        from signal import SIGTERM
        self.do_termination_test(lambda daemon: os.kill(daemon.pid, SIGTERM))


class WorkerTests(PySparkTestCase):

    def test_cancel_task(self):
        temp = tempfile.NamedTemporaryFile(delete=True)
        temp.close()
        path = temp.name

        def sleep(x):
            import os
            import time
            with open(path, 'w') as f:
                f.write("%d %d" % (os.getppid(), os.getpid()))
            time.sleep(100)

        # start job in background thread
        def run():
            self.sc.parallelize(range(1)).foreach(sleep)
        import threading
        t = threading.Thread(target=run)
        t.daemon = True
        t.start()

        daemon_pid, worker_pid = 0, 0
        while True:
            if os.path.exists(path):
                data = open(path).read().split(' ')
                daemon_pid, worker_pid = map(int, data)
                break
            time.sleep(0.1)

        # cancel jobs
        self.sc.cancelAllJobs()
        t.join()

        for i in range(50):
            try:
                os.kill(worker_pid, 0)
                time.sleep(0.1)
            except OSError:
                break  # worker was killed
        else:
            self.fail("worker has not been killed after 5 seconds")

        try:
            os.kill(daemon_pid, 0)
        except OSError:
            self.fail("daemon had been killed")

        # run a normal job
        rdd = self.sc.parallelize(range(100), 1)
        self.assertEqual(100, rdd.map(str).count())

    def test_after_exception(self):
        def raise_exception(_):
            raise Exception()
        rdd = self.sc.parallelize(range(100), 1)
        self.assertRaises(Exception, lambda: rdd.foreach(raise_exception))
        self.assertEqual(100, rdd.map(str).count())

    def test_after_jvm_exception(self):
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write("Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name, 1)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        self.assertRaises(Exception, lambda: filtered_data.count())

        rdd = self.sc.parallelize(range(100), 1)
        self.assertEqual(100, rdd.map(str).count())

    def test_accumulator_when_reuse_worker(self):
        from pyspark.accumulators import INT_ACCUMULATOR_PARAM
        acc1 = self.sc.accumulator(0, INT_ACCUMULATOR_PARAM)
        self.sc.parallelize(range(100), 20).foreach(lambda x: acc1.add(x))
        self.assertEqual(sum(range(100)), acc1.value)

        acc2 = self.sc.accumulator(0, INT_ACCUMULATOR_PARAM)
        self.sc.parallelize(range(100), 20).foreach(lambda x: acc2.add(x))
        self.assertEqual(sum(range(100)), acc2.value)
        self.assertEqual(sum(range(100)), acc1.value)

    def test_reuse_worker_after_take(self):
        rdd = self.sc.parallelize(range(100000), 1)
        self.assertEqual(0, rdd.first())

        def count():
            try:
                rdd.count()
            except Exception:
                pass

        t = threading.Thread(target=count)
        t.daemon = True
        t.start()
        t.join(5)
        self.assertTrue(not t.isAlive())
        self.assertEqual(100000, rdd.count())


class SparkSubmitTests(unittest.TestCase):

    def setUp(self):
        self.programDir = tempfile.mkdtemp()
        self.sparkSubmit = os.path.join(os.environ.get("SPARK_HOME"), "bin", "spark-submit")

    def tearDown(self):
        shutil.rmtree(self.programDir)

    def createTempFile(self, name, content):
        """
        Create a temp file with the given name and content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r'^ *\|', re.MULTILINE)
        content = re.sub(pattern, '', content.strip())
        path = os.path.join(self.programDir, name)
        with open(path, "w") as f:
            f.write(content)
        return path

    def createFileInZip(self, name, content):
        """
        Create a zip archive containing a file with the given content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r'^ *\|', re.MULTILINE)
        content = re.sub(pattern, '', content.strip())
        path = os.path.join(self.programDir, name + ".zip")
        zip = zipfile.ZipFile(path, 'w')
        zip.writestr(name, content)
        zip.close()
        return path

    def test_single_script(self):
        """Submit and test a single script file"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()
            """)
        proc = subprocess.Popen([self.sparkSubmit, script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out)

    def test_script_with_local_functions(self):
        """Submit and test a single script file calling a global function"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 3
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(foo).collect()
            """)
        proc = subprocess.Popen([self.sparkSubmit, script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[3, 6, 9]", out)

    def test_module_dependency(self):
        """Submit and test a script with a dependency on another module"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(myfunc).collect()
            """)
        zip = self.createFileInZip("mylib.py", """
            |def myfunc(x):
            |    return x + 1
            """)
        proc = subprocess.Popen([self.sparkSubmit, "--py-files", zip, script],
                                stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out)

    def test_module_dependency_on_cluster(self):
        """Submit and test a script with a dependency on another module on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(myfunc).collect()
            """)
        zip = self.createFileInZip("mylib.py", """
            |def myfunc(x):
            |    return x + 1
            """)
        proc = subprocess.Popen([self.sparkSubmit, "--py-files", zip, "--master",
                                "local-cluster[1,1,512]", script],
                                stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out)

    def test_single_script_on_cluster(self):
        """Submit and test a single script on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 2
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(foo).collect()
            """)
        # this will fail if you have different spark.executor.memory
        # in conf/spark-defaults.conf
        proc = subprocess.Popen(
            [self.sparkSubmit, "--master", "local-cluster[1,1,512]", script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out)


class ContextTests(unittest.TestCase):

    def test_failed_sparkcontext_creation(self):
        # Regression test for SPARK-1550
        self.assertRaises(Exception, lambda: SparkContext("an-invalid-master-name"))

    def test_stop(self):
        sc = SparkContext()
        self.assertNotEqual(SparkContext._active_spark_context, None)
        sc.stop()
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with(self):
        with SparkContext() as sc:
            self.assertNotEqual(SparkContext._active_spark_context, None)
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with_exception(self):
        try:
            with SparkContext() as sc:
                self.assertNotEqual(SparkContext._active_spark_context, None)
                raise Exception()
        except:
            pass
        self.assertEqual(SparkContext._active_spark_context, None)

    def test_with_stop(self):
        with SparkContext() as sc:
            self.assertNotEqual(SparkContext._active_spark_context, None)
            sc.stop()
        self.assertEqual(SparkContext._active_spark_context, None)


@unittest.skipIf(not _have_scipy, "SciPy not installed")
class SciPyTests(PySparkTestCase):

    """General PySpark tests that depend on scipy """

    def test_serialize(self):
        from scipy.special import gammaln
        x = range(1, 5)
        expected = map(gammaln, x)
        observed = self.sc.parallelize(x).map(gammaln).collect()
        self.assertEqual(expected, observed)


@unittest.skipIf(not _have_numpy, "NumPy not installed")
class NumPyTests(PySparkTestCase):

    """General PySpark tests that depend on numpy """

    def test_statcounter_array(self):
        x = self.sc.parallelize([np.array([1.0, 1.0]), np.array([2.0, 2.0]), np.array([3.0, 3.0])])
        s = x.stats()
        self.assertSequenceEqual([2.0, 2.0], s.mean().tolist())
        self.assertSequenceEqual([1.0, 1.0], s.min().tolist())
        self.assertSequenceEqual([3.0, 3.0], s.max().tolist())
        self.assertSequenceEqual([1.0, 1.0], s.sampleStdev().tolist())


if __name__ == "__main__":
    if not _have_scipy:
        print "NOTE: Skipping SciPy tests as it does not seem to be installed"
    if not _have_numpy:
        print "NOTE: Skipping NumPy tests as it does not seem to be installed"
    unittest.main()
    if not _have_scipy:
        print "NOTE: SciPy tests were skipped as it does not seem to be installed"
    if not _have_numpy:
        print "NOTE: NumPy tests were skipped as it does not seem to be installed"

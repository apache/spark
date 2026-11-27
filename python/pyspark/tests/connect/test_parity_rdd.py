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
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest import mock

from pyspark.errors import PySparkNotImplementedError
from pyspark.errors.exceptions.connect import PythonException
from pyspark.sql import Row
from pyspark.storagelevel import StorageLevel
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.utils import Py4JJavaError, QuietTest, have_numpy, have_pandas
from pyspark.tests.test_rdd import RDDTestsMixin

_CONNECT_STOPITER_EXCEPTIONS = (Py4JJavaError, RuntimeError, PythonException)

_CONNECT_TO_LOCAL_ITERATOR_TEST_NUM_PARTITIONS = 4


class RDDParityTests(RDDTestsMixin, ReusedConnectTestCase):
    @property
    def sc(self):
        return self.spark.sparkContext

    def test_persist_rdd_of_dataframe_keeps_dataframe_cache(self):
        df = self.spark.range(3)
        df.cache()
        try:
            level = df.storageLevel
            rdd = df.rdd
            self.assertEqual(rdd.getStorageLevel(), StorageLevel.NONE)
            rdd.cache()
            self.assertEqual(rdd.collect(), df.collect())
            rdd.unpersist()
            self.assertEqual(df.storageLevel, level)
        finally:
            df.unpersist()

    def test_parallelize_chained_maps_fused_logical_results(self):
        """parallelize attaches a stable partition column; chained map preserves semantics."""
        rdd = (
            self.sc.parallelize([1, 2, 3], 3)
            .map(lambda x: x + 1)
            .map(lambda x: x * 10)
            .filter(lambda x: x < 35)
        )
        self.assertEqual(sorted(rdd.collect()), [20, 30])
        self.assertEqual(rdd.count(), 2)

    @unittest.skip("Spark Connect does not support SparkContext.broadcast / Broadcast variables.")
    def test_large_broadcast(self):
        super().test_large_broadcast()

    @unittest.skip("Spark Connect does not support SparkContext.broadcast / Broadcast variables.")
    def test_unpersist(self):
        super().test_unpersist()

    @unittest.skip("Spark Connect does not support SparkContext.broadcast / Broadcast variables.")
    def test_multiple_broadcasts(self):
        super().test_multiple_broadcasts()

    @unittest.skip("Spark Connect does not support SparkContext.broadcast / Broadcast variables.")
    def test_multithread_broadcast_pickle(self):
        super().test_multithread_broadcast_pickle()

    def test_to_localiterator(self):
        """Same checks as mixin, but bounded repartition for Spark Connect throughput.

        Classic uses ``repartition(1000)`` to exercise many empty partitions cheaply on the JVM;
        Spark Connect performs a separate streamed execute cycle per logical partition without
        ``prefetchPartitions``, so repeat the assertions with an explicit modest partition count.
        """
        rdd = self.sc.parallelize([1, 2, 3])
        it = rdd.toLocalIterator()
        self.assertEqual([1, 2, 3], sorted(it))

        rdd2 = rdd.repartition(_CONNECT_TO_LOCAL_ITERATOR_TEST_NUM_PARTITIONS)
        it2 = rdd2.toLocalIterator()
        self.assertEqual([1, 2, 3], sorted(it2))

    def test_count_approx_distinct(self):
        # Spark Connect counts with Spark SQL's approx_count_distinct, whose HyperLogLog++ hashes
        # values with XxHash64 rather than the MurmurHash of Spark Classic's RDD, so estimates of
        # a few distinct values can be off by a couple in either direction.
        rdd = self.sc.parallelize(range(1000))
        self.assertTrue(950 < rdd.countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(float).countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(str).countApproxDistinct(0.03) < 1050)
        self.assertTrue(950 < rdd.map(lambda x: (x, -x)).countApproxDistinct(0.03) < 1050)

        rdd = self.sc.parallelize([i % 20 for i in range(1000)], 7)
        self.assertTrue(17 <= rdd.countApproxDistinct() <= 23)
        self.assertTrue(17 <= rdd.map(float).countApproxDistinct() <= 23)
        self.assertTrue(17 <= rdd.map(str).countApproxDistinct() <= 23)
        self.assertTrue(17 <= rdd.map(lambda x: (x, -x)).countApproxDistinct() <= 23)

        self.assertRaises(ValueError, lambda: rdd.countApproxDistinct(0.00000001))

    def test_stopiteration_in_user_code(self):
        def stopit(*x):
            raise StopIteration()

        seq_rdd = self.sc.parallelize(range(10))
        keyed_rdd = self.sc.parallelize((x % 2, x) for x in range(10))
        msg = (
            r"(Caught StopIteration thrown from user's code; failing the task"
            r"|Caught StopIteration.*from user's code"
            r"|StopIteration)"
        )

        self.assertRaisesRegex(_CONNECT_STOPITER_EXCEPTIONS, msg, seq_rdd.map(stopit).collect)
        self.assertRaisesRegex(_CONNECT_STOPITER_EXCEPTIONS, msg, seq_rdd.filter(stopit).collect)
        self.assertRaisesRegex(_CONNECT_STOPITER_EXCEPTIONS, msg, seq_rdd.foreach, stopit)
        self.assertRaisesRegex(_CONNECT_STOPITER_EXCEPTIONS, msg, seq_rdd.reduce, stopit)
        self.assertRaisesRegex(_CONNECT_STOPITER_EXCEPTIONS, msg, seq_rdd.fold, 0, stopit)
        self.assertRaisesRegex(_CONNECT_STOPITER_EXCEPTIONS, msg, seq_rdd.foreach, stopit)
        self.assertRaisesRegex(
            _CONNECT_STOPITER_EXCEPTIONS,
            msg,
            seq_rdd.cartesian(seq_rdd).flatMap(stopit).collect,
        )

        self.assertRaisesRegex(
            _CONNECT_STOPITER_EXCEPTIONS,
            msg,
            keyed_rdd.reduceByKeyLocally,
            stopit,
        )
        self.assertRaisesRegex(
            _CONNECT_STOPITER_EXCEPTIONS,
            msg,
            seq_rdd.aggregate,
            0,
            stopit,
            lambda *x: 1,
        )
        self.assertRaisesRegex(
            _CONNECT_STOPITER_EXCEPTIONS,
            msg,
            seq_rdd.aggregate,
            0,
            lambda *x: 1,
            stopit,
        )

    def test_pipe_functions(self):
        data = ["1", "2", "3"]
        rdd = self.sc.parallelize(data)
        with QuietTest(self.sc):
            self.assertEqual([], rdd.pipe("java").collect())
            self.assertRaises(Exception, rdd.pipe("java", checkCode=True).collect)
        result = rdd.pipe("cat").collect()
        result.sort()
        for x, y in zip(data, result):
            self.assertEqual(x, y)
        self.assertRaises(Exception, rdd.pipe("grep 4", checkCode=True).collect)
        self.assertEqual([], rdd.pipe("grep 4").collect())

    def test_take_on_jrdd(self):
        # Spark Classic takes from the JVM RDD, `_jrdd`, which a Spark Connect RDD does not have:
        # take from the RDD itself (SPARK-6294).
        rdd = self.sc.parallelize(range(1 << 20)).map(lambda x: str(x))
        self.assertEqual(rdd.first(), "0")

    def test_null_in_rdd(self):
        self.assertEqual(["a", None, "b"], self.sc.parallelize(["a", None, "b"]).collect())
        self.assertEqual([b"a", None, b"b"], self.sc.parallelize([b"a", None, b"b"]).collect())

    def test_multiple_python_java_RDD_conversions(self):
        data = [("1", {"director": "David Lean"}), ("2", {"director": "Andrew Dominik"})]
        self.assertEqual(2, self.sc.parallelize(data).count())

    @unittest.skipIf(not have_numpy or not have_pandas, "NumPy or Pandas not installed")
    def test_take_on_jrdd_with_large_rows_should_not_cause_deadlock(self):
        import numpy as np
        import pandas as pd

        num_rows = 100000
        num_columns = 134
        data = np.zeros((num_rows, num_columns))
        columns = map(str, range(num_columns))
        df = self.spark.createDataFrame(pd.DataFrame(data, columns=list(columns)))
        row = df.limit(1).collect()[0]
        actual = [list(row)]
        expected = [list(data[0])]
        self.assertEqual(expected, actual)

    def test_dataframe_rdd(self):
        df = self.spark.createDataFrame([(1, "a"), (2, "b")], "x int, y string")
        self.assertEqual(df.rdd.collect(), [Row(x=1, y="a"), Row(x=2, y="b")])
        self.assertEqual(df.rdd.map(lambda r: (r.x + 1, r.y)).collect(), [(2, "a"), (3, "b")])
        self.assertEqual(df.rdd.toDF().collect(), df.collect())
        self.assertEqual(
            df.rdd.map(lambda r: Row(z=r.x * 10)).toDF().collect(), [Row(z=10), Row(z=20)]
        )

    def test_application_id(self):
        self.assertEqual(self.sc.applicationId, self.spark.conf.get("spark.app.id"))

    def test_list_files_and_archives(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "added_%s.txt" % uuid.uuid4().hex)
            with open(path, "w") as f:
                f.write("x")
            self.sc.addFile(path)
            self.assertIn("file:" + path, self.sc.listFiles)

    def test_profiles(self):
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            self.sc.parallelize(range(10), 2).map(lambda x: x + 1).collect()
        try:
            with tempfile.TemporaryDirectory() as d:
                self.sc.dump_profiles(d)
                self.assertTrue(len(os.listdir(d)) > 0)
        finally:
            self.spark.profile.clear()

    def test_spark_context_serializer(self):
        # A SparkContext created on Spark Connect pickles the elements with its serializer, in
        # batches of its size, as in Spark Classic.
        from pyspark.core.connect.context import SparkContext as ConnectSparkContext
        from pyspark.serializers import BatchedSerializer, MarshalSerializer
        from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

        # The constructor creates a new session on the server of the tests, which becomes the
        # active one.
        sc = ConnectSparkContext(
            "sc://" + self.spark.client._builder.endpoint,
            appName="serializer",
            serializer=MarshalSerializer(),
            batchSize=2,
        )
        try:
            self.assertIsInstance(sc.serializer, BatchedSerializer)
            self.assertIsInstance(sc.serializer.serializer, MarshalSerializer)
            self.assertEqual(sc.serializer.batchSize, 2)
            rdd = sc.parallelize(range(10), 3)
            self.assertIs(rdd._jrdd_deserializer.serializer, sc._unbatched_serializer)
            self.assertEqual(rdd.map(lambda x: x * 2).collect(), [x * 2 for x in range(10)])
            pairs = rdd.map(lambda x: (x % 3, x)).reduceByKey(lambda a, b: a + b)
            self.assertEqual(sorted(pairs.collect()), [(0, 18), (1, 12), (2, 15)])
        finally:
            # Release the session without stop(), which also stops the local server of the tests.
            sc._session.client.release_session()
            sc._session.client.close()
            ConnectSparkSession._set_default_and_active_session(self.spark)
        self.assertEqual(self.sc.parallelize(range(3)).collect(), [0, 1, 2])

    def test_commands_do_not_import_the_client(self):
        # The Python workers of the server load the commands of the RDD API, which must not import
        # the Spark Connect client: every worker would then import grpc, protobuf and pyarrow.
        from pyspark.core.connect import rdd as connect_rdd

        commands = []

        class RecordingSerializer(connect_rdd.CloudPickleSerializer):
            # The serializer of the commands, which records them.
            def dumps(self, obj):
                command = super().dumps(obj)
                commands.append(command)
                return command

        with mock.patch.object(connect_rdd, "CloudPickleSerializer", RecordingSerializer):
            rdds = [
                self.sc.parallelize(range(10), 2).map(lambda x: (x % 3, x)).reduceByKey(max),
                self.sc.range(10).map(lambda x: (x, -x)).sortByKey(numPartitions=2),
                self.sc.range(10).zipWithIndex().groupByKey(),
                self.spark.range(3).rdd.map(lambda r: r.id),
            ]
            for rdd in rdds:
                # Building the plan of each RDD pickles its commands.
                self.assertIsNotNone(rdd._df)
        self.assertGreater(len(commands), 4)
        code = """
import sys
from pyspark.serializers import CloudPickleSerializer
before = set(sys.modules)
for path in sys.argv[1:]:
    with open(path, "rb") as f:
        CloudPickleSerializer().loads(f.read())
client = ("pyspark.core.connect.rdd", "pyspark.core.connect.context", "pyspark.sql.connect", "grpc")
print(sorted(m for m in set(sys.modules) - before if m.startswith(client)))
"""
        with tempfile.TemporaryDirectory() as d:
            paths = []
            for i, command in enumerate(commands):
                paths.append(os.path.join(d, "command-%d" % i))
                with open(paths[-1], "wb") as f:
                    f.write(command)
            env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
            result = subprocess.run(
                [sys.executable, "-c", code, *paths], env=env, capture_output=True, text=True
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "[]")

    def test_pinned_rdd_runs_its_functions_with_those_of_the_jobs(self):
        # An action of several jobs pins the RDD of the first function of a PipelinedRDD, on which
        # its functions then run, so that each job composes its function with them in one Python
        # worker, as in Spark Classic: a take after a reduceByKey runs with the merge of its values,
        # which reads the whole input of the worker, so that the worker is reused.
        from pyspark.sql.connect.plan import CachedRemoteRelation

        expected = [(0, 9), (1, 7), (2, 8)]
        rdd = self.sc.parallelize(range(10), 2).map(lambda x: (x % 3, x)).reduceByKey(max, 3)
        self.assertEqual(sorted(rdd.collect()), expected)
        self.assertIs(rdd._pinned(), rdd)
        self.assertIsInstance(rdd._prev_rdd._df._plan, CachedRemoteRelation)
        # The plan of the RDD, built before, is built again on the pinned input.
        self.assertIsNotNone(rdd._elements_df)
        self.assertIs(rdd._df_cache_input, rdd._prev_rdd._df)
        self.assertEqual(sorted(rdd.take(3)), expected)
        self.assertEqual(sorted(rdd.collect()), expected)
        # A persisted RDD keeps the plan that it persisted, which it unpersists.
        df = rdd._elements_df
        rdd.persist()
        try:
            self.assertIs(rdd._elements_df, df)
            self.assertEqual(sorted(rdd.collect()), expected)
        finally:
            rdd.unpersist()
        self.assertEqual(df.storageLevel, StorageLevel.NONE)

        # Also when getNumPartitions pinned the RDD of the first function.
        mapped = self.spark.range(10).repartition(2).rdd.map(lambda r: r.id)
        self.assertEqual(sorted(mapped.collect()), list(range(10)))
        mapped.getNumPartitions()
        self.assertIsInstance(mapped._prev_rdd._df._plan, CachedRemoteRelation)
        self.assertEqual(sorted(mapped.take(10)), list(range(10)))
        # The plan of the RDD is built again on the pinned input when it is used.
        self.assertIsNotNone(mapped._elements_df)
        self.assertIs(mapped._df_cache_input, mapped._prev_rdd._df)
        self.assertEqual(sorted(mapped.collect()), list(range(10)))

        # A persisted RDD is not pinned: its jobs share its cache.
        cached = self.sc.parallelize(range(10), 2).map(lambda x: (x % 3, x)).reduceByKey(max, 3)
        cached.cache()
        try:
            self.assertIs(cached._pinned(), cached)
            self.assertNotIsInstance(cached._prev_rdd._df._plan, CachedRemoteRelation)
            self.assertEqual(sorted(cached.take(3)), expected)
        finally:
            cached.unpersist()

    def test_set_checkpoint_dir_not_supported(self):
        with self.assertRaises(PySparkNotImplementedError):
            self.sc.setCheckpointDir("/tmp/connect-rdd-checkpoint")


if __name__ == "__main__":
    from pyspark.testing import main

    main()

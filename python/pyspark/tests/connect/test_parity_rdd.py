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

from py4j.protocol import Py4JJavaError

from pyspark.errors.exceptions.connect import PythonException
from pyspark.tests.test_rdd import RDDTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.utils import QuietTest, have_numpy, have_pandas

_CONNECT_STOPITER_EXCEPTIONS = (Py4JJavaError, RuntimeError, PythonException)

_CONNECT_TO_LOCAL_ITERATOR_TEST_NUM_PARTITIONS = 4


class RDDParityTests(RDDTestsMixin, ReusedConnectTestCase):
    @property
    def sc(self):
        return self.spark.sparkContext

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

    @unittest.skip(
        "Spark Connect parity: skips very large shuffle/load test covered by classic "
        "`pyspark.tests.test_rdd` (times out Connect CI module budget)."
    )
    def test_external_group_by_key(self):
        super().test_external_group_by_key()

    @unittest.skip(
        "Spark Connect parity: skips oversized row payload test covered by classic "
        "`pyspark.tests.test_rdd` (slow over Connect)."
    )
    def test_zip_with_different_object_sizes(self):
        super().test_zip_with_different_object_sizes()

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

    @unittest.skip(
        "Spark Connect RDD does not support ``toLocalIterator(prefetchPartitions=True)``; "
        "covered by classic ``pyspark.tests.test_rdd``."
    )
    def test_to_localiterator_prefetch(self):
        super().test_to_localiterator_prefetch()

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


if __name__ == "__main__":
    from pyspark.testing import main

    main()

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
(SPARK-51705) End-to-end tests for ``SparkSession.broadcast()`` over Spark Connect.

These tests require a running Spark Connect server (they build a real session via
:class:`ReusedConnectTestCase`), so they are gated on ``should_test_connect``.
"""

import unittest

from pyspark.errors import PySparkException
from pyspark.testing.connectutils import (
    should_test_connect,
    ReusedConnectTestCase,
    connect_requirement_message,
)

if should_test_connect:
    from pyspark.sql.connect.broadcast import ConnectBroadcast
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectBroadcastTests(ReusedConnectTestCase):
    def test_broadcast_returns_connect_broadcast(self):
        # spark.broadcast(value) returns a ConnectBroadcast that keeps the value locally for
        # driver-side reads.
        value = {"a": 1, "b": 2}
        bcast = self.spark.broadcast(value)
        self.assertIsInstance(bcast, ConnectBroadcast)
        self.assertEqual(bcast.value, value)

    def test_broadcast_in_udf(self):
        # The exact case that fails today with BROADCAST_VARIABLE_NOT_LOADED on Serverless:
        # a dict broadcast referenced from a Python UDF over a DataFrame.
        mapping = {i: i * 10 for i in range(1000)}
        bcast = self.spark.broadcast(mapping)

        @udf(returnType=StringType())
        def lookup(key):
            return str(bcast.value.get(key, -1))

        df = self.spark.range(0, 100)
        rows = df.select(lookup(df.id).alias("v")).collect()
        expected = [str(mapping.get(i, -1)) for i in range(100)]
        self.assertEqual([r.v for r in rows], expected)

    def test_broadcast_portability(self):
        # A UDF referencing a broadcast must produce byte-identical results regardless of how the
        # broadcast handle was created; here we just assert repeatable correctness across a large
        # broadcast value.
        mapping = {i: f"v{i}" for i in range(10_000)}
        bcast = self.spark.broadcast(mapping)

        @udf(returnType=StringType())
        def lookup(key):
            return bcast.value.get(key, "")

        df = self.spark.range(0, 5000)
        rows = df.select(lookup(df.id).alias("v")).collect()
        self.assertEqual([r.v for r in rows], [mapping[i] for i in range(5000)])

    def test_unpersist_and_destroy(self):
        # unpersist()/destroy() route to the server; they must not raise. After destroy() the
        # broadcast is removed from the session registry (verified server-side / via close sweep).
        bcast = self.spark.broadcast(list(range(100)))
        bcast.unpersist()
        bcast.unpersist(blocking=True)
        bcast.destroy()

    def test_broadcast_not_found(self):
        # Referencing a broadcast id that was destroyed (or never created on this session) must
        # fail loudly with BROADCAST_NOT_FOUND rather than silently emitting an empty broadcast
        # list (which would surface as BROADCAST_VARIABLE_NOT_LOADED on the worker).
        bcast = self.spark.broadcast({"x": 1})
        bcast.destroy()

        @udf(returnType=StringType())
        def lookup(key):
            return str(bcast.value.get("x", -1))

        df = self.spark.range(0, 10)
        with self.assertRaises(PySparkException):
            df.select(lookup(df.id).alias("v")).collect()


if __name__ == "__main__":
    from pyspark.testing import main

    main()

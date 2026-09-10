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

from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
from pyspark.testing.sqlutils import ReusedSQLTestCase


class PipelinedShuffleTests(ReusedSQLTestCase):
    @classmethod
    def master(cls):
        return "local[8]"

    @classmethod
    def conf(cls):
        return (
            super()
            .conf()
            .set(
                "spark.shuffle.manager.incremental",
                "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager",
            )
            .set("spark.sql.shuffle.localPipelined.enabled", "true")
            .set("spark.sql.shuffle.partitions", "4")
            .set("spark.sql.classic.shuffleDependency.fileCleanup.enabled", "false")
        )

    def assert_regular_rdd(self, rdd):
        pending = [rdd]
        visited = set()
        while pending:
            current = pending.pop()
            if current.id() in visited:
                continue
            visited.add(current.id())
            dependencies = current.dependencies().iterator()
            while dependencies.hasNext():
                dependency = dependencies.next()
                self.assertNotIn("PipelinedShuffleDependency", dependency.getClass().getName())
                pending.append(dependency.rdd())

    def test_rdd_consumers(self):
        for aqe in ("false", "true"):
            with self.subTest(aqe=aqe), self.sql_conf({"spark.sql.adaptive.enabled": aqe}):
                df = self.spark.range(0, 4000, 1, 2).repartition(4)
                self.assertEqual(len(df.collect()), 4000)
                rdd = df.rdd
                # Check lineage before running consumers that would hang with a channel.
                self.assert_regular_rdd(rdd._jrdd.rdd())
                self.assertEqual(rdd.coalesce(1).count(), 4000)
                self.assertEqual(
                    sorted(
                        rdd.map(lambda r: (r.id % 2, 1)).reduceByKey(lambda a, b: a + b).collect()
                    ),
                    [(0, 2000), (1, 2000)],
                )

    def test_iterator_reuses_shuffle_output(self):
        for aqe in ("false", "true"):
            for prefetch in (False, True):
                with (
                    self.subTest(aqe=aqe, prefetch=prefetch),
                    self.sql_conf({"spark.sql.adaptive.enabled": aqe}),
                ):
                    evaluated = self.sc.accumulator(0)

                    @udf(LongType())
                    def record_row(value):
                        evaluated.add(1)
                        return value

                    df = (
                        self.spark.range(0, 1000, 1, 2)
                        .select(record_row("id").alias("id"))
                        .repartition(4)
                    )
                    self.assertEqual(len(df.collect()), 1000)
                    self.assertEqual(evaluated.value, 1000)
                    for run in (1, 2):
                        rows = list(df.toLocalIterator(prefetchPartitions=prefetch))
                        self.assertEqual(sorted(r.id for r in rows), list(range(1000)))
                        self.assertEqual(evaluated.value, (run + 1) * 1000)
                    self.assertEqual(
                        self.spark.conf.get("spark.sql.shuffle.localPipelined.enabled"), "true"
                    )


if __name__ == "__main__":
    unittest.main()

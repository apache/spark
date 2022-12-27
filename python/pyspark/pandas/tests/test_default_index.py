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

import pandas as pd

from pyspark.sql import functions as F
from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class DefaultIndexTest(PandasOnSparkTestCase):
    def test_default_index_sequence(self):
        with ps.option_context("compute.default_index_type", "sequence"):
            sdf = self.spark.range(1000)
            self.assert_eq(ps.DataFrame(sdf), pd.DataFrame({"id": list(range(1000))}))

    def test_default_index_distributed_sequence(self):
        with ps.option_context("compute.default_index_type", "distributed-sequence"):
            sdf = self.spark.range(1000)
            self.assert_eq(ps.DataFrame(sdf), pd.DataFrame({"id": list(range(1000))}))

    def test_default_index_distributed(self):
        with ps.option_context("compute.default_index_type", "distributed"):
            sdf = self.spark.range(1000)
            pdf = ps.DataFrame(sdf)._to_pandas()
            self.assertEqual(len(set(pdf.index)), len(pdf))

    def test_index_distributed_sequence_cleanup(self):
        with ps.option_context(
            "compute.default_index_type", "distributed-sequence"
        ), ps.option_context("compute.ops_on_diff_frames", True):

            with ps.option_context("compute.default_index_cache", "LOCAL_CHECKPOINT"):
                cached_rdd_ids = [rdd_id for rdd_id in self.spark._jsc.getPersistentRDDs()]

                psdf1 = (
                    self.spark.range(0, 100, 1, 10).withColumn("Key", F.col("id") % 33).pandas_api()
                )

                psdf2 = psdf1["Key"].reset_index()
                psdf2["index"] = (psdf2.groupby(["Key"]).cumcount() == 0).astype(int)
                psdf2["index"] = psdf2["index"].cumsum()

                psdf3 = ps.merge(psdf1, psdf2, how="inner", left_on=["Key"], right_on=["Key"])
                _ = len(psdf3)

                # newly cached rdd
                self.assertTrue(
                    any(
                        rdd_id not in cached_rdd_ids
                        for rdd_id in self.spark._jsc.getPersistentRDDs()
                    )
                )

            for storage_level in ["NONE", "DISK_ONLY_2", "MEMORY_AND_DISK_SER"]:
                with ps.option_context("compute.default_index_cache", storage_level):
                    cached_rdd_ids = [rdd_id for rdd_id in self.spark._jsc.getPersistentRDDs()]

                    psdf1 = (
                        self.spark.range(0, 100, 1, 10)
                        .withColumn("Key", F.col("id") % 33)
                        .pandas_api()
                    )

                    psdf2 = psdf1["Key"].reset_index()
                    psdf2["index"] = (psdf2.groupby(["Key"]).cumcount() == 0).astype(int)
                    psdf2["index"] = psdf2["index"].cumsum()

                    psdf3 = ps.merge(psdf1, psdf2, how="inner", left_on=["Key"], right_on=["Key"])
                    _ = len(psdf3)

                    # no newly cached rdd
                    self.assertTrue(
                        all(
                            rdd_id in cached_rdd_ids
                            for rdd_id in self.spark._jsc.getPersistentRDDs()
                        )
                    )


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_default_index import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

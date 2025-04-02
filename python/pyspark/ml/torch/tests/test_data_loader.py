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

from pyspark.ml.torch.distributor import (
    TorchDistributor,
    _get_spark_partition_data_loader,
)
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors


# @unittest.skipIf(not have_torch, torch_requirement_message)
# TODO(SPARK-50864): Re-enable this test after fixing the slowness
@unittest.skip("Disabled due to slowness")
class TorchDistributorDataLoaderUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.default.parallelism", "1")
            .getOrCreate()
        )

    def tearDown(self) -> None:
        self.spark.stop()

    def _check_data_loader_result_correctness(self, result, expected):
        import numpy as np

        assert len(result) == len(expected)

        for res_row, exp_row in zip(result, expected):
            assert len(res_row) == len(exp_row)
            for res_field, exp_field in zip(res_row, exp_row):
                np.testing.assert_almost_equal(res_field.numpy(), exp_field)

    def test_data_loader(self):
        spark_df = self.spark.createDataFrame(
            [
                (Vectors.dense([1.0, 2.0, 3.5]), 0, 10.5),
                (Vectors.sparse(3, [1, 2], [4.5, 5.5]), 3, 12.5),
                (Vectors.dense([6.0, 7.0, 8.5]), 1, 1.5),
                (Vectors.sparse(3, [0, 2], [-2.5, -6.5]), 2, 9.5),
            ],
            schema=["features", "label", "weight"],
        )

        torch_distributor = TorchDistributor(local_mode=False, use_gpu=False)

        def train_function(num_samples, batch_size):
            data_loader = _get_spark_partition_data_loader(num_samples, batch_size)
            return list(data_loader)

        result = torch_distributor._train_on_dataframe(
            train_function,
            spark_df,
            num_samples=4,
            batch_size=2,
        )
        self._check_data_loader_result_correctness(
            result,
            [
                [[[1.0, 2.0, 3.5], [0.0, 4.5, 5.5]], [0, 3], [10.5, 12.5]],
                [[[6.0, 7.0, 8.5], [-2.5, 0.0, -6.5]], [1, 2], [1.5, 9.5]],
            ],
        )

        result = torch_distributor._train_on_dataframe(
            train_function,
            spark_df,
            num_samples=4,
            batch_size=3,
        )
        self._check_data_loader_result_correctness(
            result,
            [
                [
                    [[1.0, 2.0, 3.5], [0.0, 4.5, 5.5], [6.0, 7.0, 8.5]],
                    [0, 3, 1],
                    [10.5, 12.5, 1.5],
                ],
                [[[-2.5, 0.0, -6.5]], [2], [9.5]],
            ],
        )

        result = torch_distributor._train_on_dataframe(
            train_function,
            spark_df,
            num_samples=6,
            batch_size=3,
        )
        self._check_data_loader_result_correctness(
            result,
            [
                [
                    [[1.0, 2.0, 3.5], [0.0, 4.5, 5.5], [6.0, 7.0, 8.5]],
                    [0, 3, 1],
                    [10.5, 12.5, 1.5],
                ],
                [
                    [[-2.5, 0.0, -6.5], [1.0, 2.0, 3.5], [0.0, 4.5, 5.5]],
                    [2, 0, 3],
                    [9.5, 10.5, 12.5],
                ],
            ],
        )


if __name__ == "__main__":
    from pyspark.ml.torch.tests.test_data_loader import *  # noqa: F401,F403

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

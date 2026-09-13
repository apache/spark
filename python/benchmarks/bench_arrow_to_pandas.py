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
Microbenchmarks for ``ArrowBatchTransformer.to_pandas``, the hot path of pandas
UDF inputs: every pandas UDF eval type calls it once per batch to build the
Series it passes to the user's function.

Part of the per-batch cost is fixed per COLUMN and does not scale with row
count, so ``n_cols`` is swept alongside ``n_rows``: wide batches and small
batches are the shapes where that fixed cost dominates.

``ArrowArrayToPandasConversion.convert`` routes each column by type. ``long`` and
``timestamp`` are in the ``_prefer_convert_numpy`` allowlist and take
``convert_numpy``; ``string`` is not, and takes ``convert_legacy``. The two
allowlist types differ by an order of magnitude in conversion cost -- a timestamp
column is localized by pyarrow compute kernels -- so sweeping both shows how much
of a change is fixed per-column cost rather than per-row work.
"""

import numpy as np
import pyarrow as pa


class ArrowBatchToPandasBenchmark:
    """Benchmark ``ArrowBatchTransformer.to_pandas`` over a whole RecordBatch."""

    params = [
        [128, 10000],
        [1, 50],
        ["long", "timestamp", "string"],
    ]
    param_names = ["n_rows", "n_cols", "col_type"]

    def setup(self, n_rows, n_cols, col_type):
        from pyspark.sql.conversion import ArrowBatchTransformer
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        if col_type == "long":
            column = pa.array(np.arange(n_rows, dtype=np.int64))
            spark_type = LongType()
        elif col_type == "timestamp":
            base = np.datetime64("2020-01-01T00:00:00", "us")
            column = pa.array(base + np.arange(n_rows) * np.timedelta64(1, "s")).cast(
                pa.timestamp("us", tz="UTC")
            )
            spark_type = TimestampType()
        elif col_type == "string":
            column = pa.array([f"s{i:07d}" for i in range(n_rows)], type=pa.string())
            spark_type = StringType()
        else:
            raise ValueError(f"unknown col_type: {col_type}")

        names = [f"c{i}" for i in range(n_cols)]
        self.batch = pa.RecordBatch.from_arrays([column] * n_cols, names)
        self.schema = StructType([StructField(name, spark_type) for name in names])
        self.to_pandas = ArrowBatchTransformer.to_pandas

    def time_batch_to_pandas(self, n_rows, n_cols, col_type):
        self.to_pandas(self.batch, timezone="UTC", schema=self.schema)

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
Microbenchmarks for Arrow to Pandas conversions.
"""

import numpy as np
import pandas as pd
import pyarrow as pa


class ArrowToPandasBenchmark:
    """Benchmark for Arrow int array -> Pandas conversions with different types_mapper."""

    params = [
        [10000, 100000, 1000000],
        ["default", "arrow_dtype"],
    ]
    param_names = ["n_rows", "types_mapper"]

    def setup(self, n_rows, types_mapper):
        self.int_array = pa.array(np.random.randint(0, 1000, n_rows))
        self.int_array_with_nulls = pa.array(
            [i if i % 10 != 0 else None for i in range(n_rows)]
        )
        self.types_mapper = pd.ArrowDtype if types_mapper == "arrow_dtype" else None

    def time_int_to_pandas(self, n_rows, types_mapper):
        self.int_array.to_pandas(types_mapper=self.types_mapper)

    def time_int_with_nulls_to_pandas(self, n_rows, types_mapper):
        self.int_array_with_nulls.to_pandas(types_mapper=self.types_mapper)

    def peakmem_int_to_pandas(self, n_rows, types_mapper):
        self.int_array.to_pandas(types_mapper=self.types_mapper)

    def peakmem_int_with_nulls_to_pandas(self, n_rows, types_mapper):
        self.int_array_with_nulls.to_pandas(types_mapper=self.types_mapper)


class LongArrowToPandasBenchmark:
    """Benchmark for Arrow long array -> Pandas conversions."""

    params = [
        [10000, 100000, 1000000],
        ["simple", "arrow_types_mapper", "pd.Series"],
    ]
    param_names = ["n_rows", "method"]

    def setup(self, n_rows, method):
        self.long_array = pa.array(list(range(n_rows - 1)) + [9223372036854775707], type=pa.int64())

    # check 3 different ways to convert non-nullable longs to numpy int64
    def run_long_to_pandas(self, n_rows, method):
        if method == "simple":
            ser = self.long_array.to_pandas()
        elif method == "arrow_types_mapper":
            ser = self.long_array.to_pandas(types_mapper=pd.ArrowDtype).astype(np.int64)
        else:
            ser = pd.Series(self.long_array, dtype=np.int64)
        assert ser.dtype == np.int64

    def time_long_to_pandas(self, n_rows, method):
        self.run_long_to_pandas(n_rows, method)

    def peakmem_long_to_pandas(self, n_rows, method):
        self.run_long_to_pandas(n_rows, method)


class NullableLongArrowToPandasBenchmark:
    """Benchmark for Arrow long array with nulls -> Pandas conversions."""

    params = [
        [10000, 100000, 1000000],
        ["integer_object_nulls", "arrow_types_mapper", "pd.Series"],
    ]
    param_names = ["n_rows", "method"]

    def setup(self, n_rows, method):
        self.long_array_with_nulls = pa.array(
            [i if i % 10 != 0 else None for i in range(n_rows - 1)] + [9223372036854775707],
            type=pa.int64(),
            )

    # check 3 different ways to convert nullable longs to nullable extension type
    def run_long_with_nulls_to_pandas_ext(self, n_rows, method):
        if method == "integer_object_nulls":
            ser = self.long_array_with_nulls.to_pandas(integer_object_nulls=True).astype(
                pd.Int64Dtype()
            )
        elif method == "arrow_types_mapper":
            ser = self.long_array_with_nulls.to_pandas(types_mapper=pd.ArrowDtype).astype(
                pd.Int64Dtype()
            )
        else:
            ser = pd.Series(self.long_array_with_nulls.to_pylist(), dtype=pd.Int64Dtype())
        assert ser.dtype == pd.Int64Dtype()

    def time_long_with_nulls_to_pandas_ext(self, n_rows, method):
        self.run_long_with_nulls_to_pandas_ext(n_rows, method)

    def peakmem_long_with_nulls_to_pandas_ext(self, n_rows, method):
        self.run_long_with_nulls_to_pandas_ext(n_rows, method)

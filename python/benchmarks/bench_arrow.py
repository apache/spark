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

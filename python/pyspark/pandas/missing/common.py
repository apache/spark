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


memory_usage = lambda f: f(
    "memory_usage",
    reason="Unlike pandas, most DataFrames are not materialized in memory in Spark "
    "(and pandas-on-Spark), and as a result memory_usage() does not do what you intend it "
    "to do. Use Spark's web UI to monitor disk and memory usage of your application.",
)

array = lambda f: f(
    "array", reason="If you want to collect your data as an NumPy array, use 'to_numpy()' instead."
)

to_pickle = lambda f: f(
    "to_pickle",
    reason="For storage, we encourage you to use Delta or Parquet, instead of Python pickle "
    "format.",
)

to_xarray = lambda f: f(
    "to_xarray",
    reason="If you want to collect your data as an NumPy array, use 'to_numpy()' instead.",
)

to_list = lambda f: f(
    "to_list",
    reason="If you want to collect your data as an NumPy array, use 'to_numpy()' instead.",
)

tolist = lambda f: f(
    "tolist", reason="If you want to collect your data as an NumPy array, use 'to_numpy()' instead."
)

__iter__ = lambda f: f(
    "__iter__",
    reason="If you want to collect your data as an NumPy array, use 'to_numpy()' instead.",
)

duplicated = lambda f: f(
    "duplicated",
    reason="'duplicated' API returns np.ndarray and the data size is too large."
    "You can just use DataFrame.deduplicated instead",
)

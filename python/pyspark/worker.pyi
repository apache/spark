#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark import shuffle as shuffle
from pyspark.broadcast import Broadcast as Broadcast
from pyspark.files import SparkFiles as SparkFiles
from pyspark.java_gateway import local_connect_and_auth as local_connect_and_auth
from pyspark.rdd import PythonEvalType as PythonEvalType
from pyspark.resource import ResourceInformation as ResourceInformation
from pyspark.serializers import (
    BatchedSerializer as BatchedSerializer,
    PickleSerializer as PickleSerializer,
    SpecialLengths as SpecialLengths,
    UTF8Deserializer as UTF8Deserializer,
    read_bool as read_bool,
    read_int as read_int,
    read_long as read_long,
    write_int as write_int,
    write_long as write_long,
    write_with_length as write_with_length,
)
from pyspark.sql.pandas.serializers import (
    ArrowStreamPandasUDFSerializer as ArrowStreamPandasUDFSerializer,
    CogroupUDFSerializer as CogroupUDFSerializer,
)
from pyspark.sql.pandas.types import to_arrow_type as to_arrow_type
from pyspark.sql.types import StructType as StructType
from pyspark.taskcontext import (
    BarrierTaskContext as BarrierTaskContext,
    TaskContext as TaskContext,
)
from pyspark.util import fail_on_stopiteration as fail_on_stopiteration
from typing import Any

has_resource_module: bool
pickleSer: Any
utf8_deserializer: Any

def report_times(outfile: Any, boot: Any, init: Any, finish: Any) -> None: ...
def add_path(path: Any) -> None: ...
def read_command(serializer: Any, file: Any): ...
def chain(f: Any, g: Any): ...
def wrap_udf(f: Any, return_type: Any): ...
def wrap_scalar_pandas_udf(f: Any, return_type: Any): ...
def wrap_pandas_iter_udf(f: Any, return_type: Any): ...
def wrap_cogrouped_map_pandas_udf(f: Any, return_type: Any, argspec: Any): ...
def wrap_grouped_map_pandas_udf(f: Any, return_type: Any, argspec: Any): ...
def wrap_grouped_agg_pandas_udf(f: Any, return_type: Any): ...
def wrap_window_agg_pandas_udf(
    f: Any, return_type: Any, runner_conf: Any, udf_index: Any
): ...
def wrap_unbounded_window_agg_pandas_udf(f: Any, return_type: Any): ...
def wrap_bounded_window_agg_pandas_udf(f: Any, return_type: Any): ...
def read_single_udf(
    pickleSer: Any, infile: Any, eval_type: Any, runner_conf: Any, udf_index: Any
): ...
def read_udfs(pickleSer: Any, infile: Any, eval_type: Any): ...
def main(infile: Any, outfile: Any) -> None: ...

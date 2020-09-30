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

from pyspark.sql.pandas.utils import (  # noqa: F401
    require_minimum_pandas_version as require_minimum_pandas_version,
)
from typing import Any, Optional

def infer_eval_type(sig: Any): ...
def check_tuple_annotation(
    annotation: Any, parameter_check_func: Optional[Any] = ...
): ...
def check_iterator_annotation(
    annotation: Any, parameter_check_func: Optional[Any] = ...
): ...
def check_union_annotation(
    annotation: Any, parameter_check_func: Optional[Any] = ...
): ...

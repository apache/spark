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
from pyspark.ml import functions as PyMLFunctions
from pyspark.sql.column import Column
from pyspark.sql.connect.functions.builtin import _invoke_function, _to_col, lit


def vector_to_array(col: Column, dtype: str = "float64") -> Column:
    return _invoke_function("vector_to_array", _to_col(col), lit(dtype))


vector_to_array.__doc__ = PyMLFunctions.vector_to_array.__doc__


def array_to_vector(col: Column) -> Column:
    return _invoke_function("array_to_vector", _to_col(col))


array_to_vector.__doc__ = PyMLFunctions.array_to_vector.__doc__

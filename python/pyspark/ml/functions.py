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

from pyspark import since, SparkContext
from pyspark.sql.column import Column, _to_java_column

@since(3.0)
def vector_to_dense_array(col):
    """
    Convert MLlib sparse/dense vectors in a DataFrame into dense arrays.

    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.functions import vector_to_dense_array
    >>> df = spark.createDataFrame([
    ...     (Vectors.dense(1.0, 2.0, 3.0),),
    ...     (Vectors.sparse(3, [(0, 2.0), (2, 3.0)]),)], ["vec"])
    >>> df.select(vector_to_dense_array("vec").alias("arr")).collect()
    [Row(arr=[1.0, 2.0, 3.0]), Row(arr=[2.0, 0.0, 3.0])]
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.org.apache.spark.ml.functions.vector_to_dense_array(_to_java_column(col)))

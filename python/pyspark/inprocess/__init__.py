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
pyspark.inprocess — In-process Python UDF framework for Apache Spark.

Provides zero-copy Arrow-based UDF execution via jep (Java Embedded Python),
eliminating the socket IPC overhead of standard Python/pandas UDFs.

Public API:
    inprocess_udf  - decorator to create an in-process Python UDF

Example::

    import pyarrow.compute as pc
    from pyspark.inprocess import inprocess_udf
    from pyspark.sql.types import LongType

    @inprocess_udf(return_type=LongType())
    def double(x):
        return pc.multiply(x, 2)

    df.select(double(df.value)).show()
"""

from pyspark.inprocess.udf import inprocess_udf

__all__ = ["inprocess_udf"]

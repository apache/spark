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
Spark Connect counterparts for :mod:`pyspark.core` helpers.

RDD-style execution backed by Spark SQL Connect lives here so imports stay separate from classic
JVM APIs: use :mod:`pyspark.core.context` (:class:`~pyspark.core.context.SparkContext`,
:class:`~pyspark.core.rdd.RDD`) versus this package's
:class:`~pyspark.core.connect.context.SparkContext` and :class:`~pyspark.core.connect.rdd.RDD`.
"""

from pyspark.sql.connect.utils import check_dependencies

check_dependencies()

from pyspark.core.connect.context import SparkContext
from pyspark.core.connect.rdd import RDD

__all__ = ["RDD", "SparkContext"]

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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import Type, TYPE_CHECKING

from pyspark.sql.datasource import DataSourceRegistration as PySparkDataSourceRegistration


if TYPE_CHECKING:
    from pyspark.sql.datasource import DataSource
    from pyspark.sql.connect.session import SparkSession


class DataSourceRegistration:
    """
    Wrapper for data source registration.

    .. versionadded: 4.0.0
    """

    def __init__(self, sparkSession: "SparkSession"):
        self.sparkSession = sparkSession

    def register(
        self,
        dataSource: Type["DataSource"],
    ) -> None:
        self.sparkSession._client.register_data_source(dataSource)

    register.__doc__ = PySparkDataSourceRegistration.register.__doc__

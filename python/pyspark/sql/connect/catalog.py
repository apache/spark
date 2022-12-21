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

from typing import NamedTuple, Optional, TYPE_CHECKING, List

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession


class Database(NamedTuple):
    name: str
    catalog: Optional[str]
    description: Optional[str]
    locationUri: str


class Catalog:
    """
    User-facing catalog API, accessible through `SparkSession.catalog`.
    """

    def __init__(self, sparkSession: "SparkSession") -> None:
        self._sparkSession = sparkSession

    def listDatabases(self) -> List[Database]:
        rows = self._sparkSession.sql("SHOW DATABASES").collect()
        databases = []
        for row in rows:
            databases.append(
                Database(name=row["namespace"], catalog=None, description=None, locationUri="")
            )
        return databases

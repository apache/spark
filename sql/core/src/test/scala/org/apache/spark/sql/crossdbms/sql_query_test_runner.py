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

from typing import List, Optional

class SQLQueryTestRunner:
    """
    Trait for classes that can run SQL queries for testing.
    This is specifically used as a wrapper around a connection to a system that can run SQL queries,
    to run queries from SQLQueryTestSuite.
    """
    def __init__(self, connection):
        self.connection = connection

    def run_query(self, query: str) -> List[str]:
        """
        Runs a given query using the JDBC connection and returns the result as a list of strings.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            A list of strings representing the output, where each element represents a single row.
        """
        return self.connection.run_query(query)

    def clean_up(self) -> None:
        """
        Closes the JDBC connection.
        """
        self.connection.close()

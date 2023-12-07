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

from abc import ABC, abstractmethod
from typing import List, Optional

import psycopg2

# Represents a connection (session) to a database using JDBC.
class DatabaseConnection(ABC):
    """
    Executes SQL queries, performs table operations, and loads data using JDBC.
    """
    @abstractmethod
    def run_query(self, query: str) -> List[str]:
        """
        Executes the given SQL query and returns the result as a sequence of strings.

        Returns:
            A list of strings representing the output, where each element represents a single row.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """
        Drops the table with the specified table name.

        Args:
            table_name (str): The name of the table to be dropped.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def create_table(self, table_name: str, schema_string: str) -> None:
        """
        Creates a table with the specified name and schema.

        Args:
            table_name (str): The name of the table to be created.
            schema_string (str): The schema definition for the table.
                                Note that this may vary depending on the database system.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def close(self) -> None:
        """
        Closes the JDBC connection.
        """
        raise NotImplementedError("Subclasses must implement this method")


# Represents a connection (session) to a PostgreSQL database.
class PostgresConnection(DatabaseConnection):
    """
    Represents a connection (session) to a PostgreSQL database using JDBC.
    """
    POSTGRES_DRIVER_CLASS_NAME: str = "org.postgresql.Driver"
    DEFAULT_USER: str = "pg"
    DEFAULT_CONNECTION_URL: str = f"dbname=postgres user={DEFAULT_USER} password=password host=localhost port=5432"

    def __init__(self, connection_url: Optional[str] = None) -> None:
        self.url: str = connection_url or self.DEFAULT_CONNECTION_URL
        self.conn: psycopg2.extensions.connection = psycopg2.connect(self.url)
        self.cursor: psycopg2.extensions.cursor = self.conn.cursor()

    def run_query(self, query: str) -> List[str]:
        try:
            self.cursor.execute(query)
            rows: List[str] = [",".join(map(str, row)) for row in self.cursor.fetchall()]
            return rows
        except Exception as e:
            return [str(e)]

    def drop_table(self, table_name: str) -> None:
        drop_table_sql: str = f"DROP TABLE IF EXISTS {table_name}"
        self.cursor.execute(drop_table_sql)
        self.conn.commit()

    def create_table(self, table_name: str, schema_string: str) -> None:
        create_table_sql: str = f"CREATE TABLE {table_name} ({schema_string})"
        self.cursor.execute(create_table_sql)
        self.conn.commit()

    def close(self) -> None:
        if not self.conn.closed:
            self.cursor.close()
            self.conn.close()

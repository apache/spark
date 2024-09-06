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
import sys
from typing import Dict, Optional, TYPE_CHECKING

from pyspark.sql.column import Column
from pyspark.sql.utils import to_scala_map

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame

__all__ = ["MergeIntoWriter"]


class MergeIntoWriter:
    """
    `MergeIntoWriter` provides methods to define and execute merge actions based
    on specified conditions.

    .. versionadded: 4.0.0
    """

    def __init__(self, df: "DataFrame", table: str, condition: Column):
        self._spark = df.sparkSession

        from pyspark.sql.classic.column import _to_java_column

        self._jwriter = df._jdf.mergeInto(table, _to_java_column(condition))

    def whenMatched(self, condition: Optional[Column] = None) -> "MergeIntoWriter.WhenMatched":
        """
        Initialize a `WhenMatched` action with a condition.

        This `WhenMatched` action will be executed when a source row matches a target table row
        based on the merge condition and the specified `condition` is satisfied.

        This `WhenMatched` can be followed by one of the following merge actions:
          - `updateAll`: Update all the matched target table rows with source dataset rows.
          - `update(Dict)`: Update all the matched target table rows while changing only
            a subset of columns based on the provided assignment.
          - `delete`: Delete all target rows that have a match in the source table.
        """
        return self.WhenMatched(self, condition)

    def whenNotMatched(
        self, condition: Optional[Column] = None
    ) -> "MergeIntoWriter.WhenNotMatched":
        """
        Initialize a `WhenNotMatched` action with a condition.

        This `WhenNotMatched` action will be executed when a source row does not match any target
        row based on the merge condition and the specified `condition` is satisfied.

        This `WhenNotMatched` can be followed by one of the following merge actions:
          - `insertAll`: Insert all rows from the source that are not already in the target table.
          - `insert(Dict)`: Insert all rows from the source that are not already in the target
            table, with the specified columns based on the provided assignment.
        """
        return self.WhenNotMatched(self, condition)

    def whenNotMatchedBySource(
        self, condition: Optional[Column] = None
    ) -> "MergeIntoWriter.WhenNotMatchedBySource":
        """
        Initialize a `WhenNotMatchedBySource` action with a condition.

        This `WhenNotMatchedBySource` action will be executed when a target row does not match any
        rows in the source table based on the merge condition and the specified `condition`
        is satisfied.

        This `WhenNotMatchedBySource` can be followed by one of the following merge actions:
          - `updateAll`: Update all the not matched target table rows with source dataset rows.
          - `update(Dict)`: Update all the not matched target table rows while changing only
            the specified columns based on the provided assignment.
          - `delete`: Delete all target rows that have no matches in the source table.
        """
        return self.WhenNotMatchedBySource(self, condition)

    def withSchemaEvolution(self) -> "MergeIntoWriter":
        """
        Enable automatic schema evolution for this merge operation.
        """
        self._jwriter = self._jwriter.withSchemaEvolution()
        return self

    def merge(self) -> None:
        """
        Execute the merge operation.
        """
        self._jwriter.merge()

    class WhenMatched:
        """
        A class for defining actions to be taken when matching rows in a DataFrame during
        a merge operation."""

        def __init__(self, writer: "MergeIntoWriter", condition: Optional[Column]):
            self.writer = writer
            if condition is None:
                self.when_matched = writer._jwriter.whenMatched()
            else:
                from pyspark.sql.classic.column import _to_java_column

                self.when_matched = writer._jwriter.whenMatched(_to_java_column(condition))

        def updateAll(self) -> "MergeIntoWriter":
            """
            Specifies an action to update all matched rows in the DataFrame.
            """
            self.writer._jwriter = self.when_matched.updateAll()
            return self.writer

        def update(self, assignments: Dict[str, Column]) -> "MergeIntoWriter":
            """
            Specifies an action to update matched rows in the DataFrame with the provided column
            assignments.
            """
            jvm = self.writer._spark._jvm
            from pyspark.sql.classic.column import _to_java_column

            jmap = to_scala_map(jvm, {k: _to_java_column(v) for k, v in assignments.items()})
            self.writer._jwriter = self.when_matched.update(jmap)
            return self.writer

        def delete(self) -> "MergeIntoWriter":
            """
            Specifies an action to delete matched rows from the DataFrame.
            """
            self.writer._jwriter = self.when_matched.delete()
            return self.writer

    class WhenNotMatched:
        """
        A class for defining actions to be taken when no matching rows are found in a DataFrame
        during a merge operation."""

        def __init__(self, writer: "MergeIntoWriter", condition: Optional[Column]):
            self.writer = writer
            if condition is None:
                self.when_not_matched = writer._jwriter.whenNotMatched()
            else:
                from pyspark.sql.classic.column import _to_java_column

                self.when_not_matched = writer._jwriter.whenNotMatched(_to_java_column(condition))

        def insertAll(self) -> "MergeIntoWriter":
            """
            Specifies an action to insert all non-matched rows into the DataFrame.
            """
            self.writer._jwriter = self.when_not_matched.insertAll()
            return self.writer

        def insert(self, assignments: Dict[str, Column]) -> "MergeIntoWriter":
            """
            Specifies an action to insert non-matched rows into the DataFrame with the provided
            column assignments.
            """
            jvm = self.writer._spark._jvm
            from pyspark.sql.classic.column import _to_java_column

            jmap = to_scala_map(jvm, {k: _to_java_column(v) for k, v in assignments.items()})
            self.writer._jwriter = self.when_not_matched.insert(jmap)
            return self.writer

    class WhenNotMatchedBySource:
        """
        A class for defining actions to be performed when there is no match by source
        during a merge operation in a MergeIntoWriter.
        """

        def __init__(self, writer: "MergeIntoWriter", condition: Optional[Column]):
            self.writer = writer
            if condition is None:
                self.when_not_matched_by_source = writer._jwriter.whenNotMatchedBySource()
            else:
                from pyspark.sql.classic.column import _to_java_column

                self.when_not_matched_by_source = writer._jwriter.whenNotMatchedBySource(
                    _to_java_column(condition)
                )

        def updateAll(self) -> "MergeIntoWriter":
            """
            Specifies an action to update all non-matched rows in the target DataFrame when
            not matched by the source.
            """
            self.writer._jwriter = self.when_not_matched_by_source.updateAll()
            return self.writer

        def update(self, assignments: Dict[str, Column]) -> "MergeIntoWriter":
            """
            Specifies an action to update non-matched rows in the target DataFrame with the provided
            column assignments when not matched by the source.
            """
            jvm = self.writer._spark._jvm
            from pyspark.sql.classic.column import _to_java_column

            jmap = to_scala_map(jvm, {k: _to_java_column(v) for k, v in assignments.items()})
            self.writer._jwriter = self.when_not_matched_by_source.update(jmap)
            return self.writer

        def delete(self) -> "MergeIntoWriter":
            """
            Specifies an action to delete matched rows from the DataFrame.
            """
            self.writer._jwriter = self.when_not_matched_by_source.delete()
            return self.writer


def _test() -> None:
    import doctest
    import os
    import py4j
    from pyspark.core.context import SparkContext
    from pyspark.sql import SparkSession
    import pyspark.sql.merge

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.merge.__dict__.copy()
    sc = SparkContext("local[4]", "PythonTest")
    try:
        spark = SparkSession._getActiveSessionOrCreate()
    except py4j.protocol.Py4JError:
        spark = SparkSession(sc)

    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.merge,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()

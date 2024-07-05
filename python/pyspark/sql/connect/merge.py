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
from typing import Dict, Optional, TYPE_CHECKING, List, Callable

from pyspark.sql.connect import proto
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.functions import expr
from pyspark.sql.merge import MergeIntoWriter as PySparkMergeIntoWriter

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.plan import LogicalPlan
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.metrics import ExecutionInfo

__all__ = ["MergeIntoWriter"]


def _build_merge_action(
    client: "SparkConnectClient",
    action_type: proto.MergeAction.ActionType.ValueType,
    condition: Optional[Column] = None,
    assignments: Optional[Dict[str, Column]] = None,
) -> proto.MergeAction:
    if assignments is None:
        proto_assignments = None
    else:
        proto_assignments = [
            proto.MergeAction.Assignment(
                key=expr(k).to_plan(client), value=v.to_plan(client)  # type: ignore[operator]
            )
            for k, v in assignments.items()
        ]
    return proto.MergeAction(
        action_type=action_type,
        condition=None if condition is None else condition.to_plan(client),
        assignments=proto_assignments,
    )


class MergeIntoWriter:
    def __init__(
        self,
        plan: "LogicalPlan",
        session: "SparkSession",
        table: str,
        condition: Column,
        callback: Optional[Callable[["ExecutionInfo"], None]] = None,
    ):
        self._client = session.client
        self._target_table = table
        self._source_plan = plan
        self._condition = condition

        self._callback = callback if callback is not None else lambda _: None
        self._schema_evolution_enabled = False
        self._matched_actions = list()  # type: List[proto.MergeAction]
        self._not_matched_actions = list()  # type: List[proto.MergeAction]
        self._not_matched_by_source_actions = list()  # type: List[proto.MergeAction]

    def whenMatched(self, condition: Optional[Column] = None) -> "MergeIntoWriter.WhenMatched":
        return self.WhenMatched(self, condition)

    whenMatched.__doc__ = PySparkMergeIntoWriter.whenMatched.__doc__

    def whenNotMatched(
        self, condition: Optional[Column] = None
    ) -> "MergeIntoWriter.WhenNotMatched":
        return self.WhenNotMatched(self, condition)

    whenNotMatched.__doc__ = PySparkMergeIntoWriter.whenNotMatched.__doc__

    def whenNotMatchedBySource(
        self, condition: Optional[Column] = None
    ) -> "MergeIntoWriter.WhenNotMatchedBySource":
        return self.WhenNotMatchedBySource(self, condition)

    whenNotMatchedBySource.__doc__ = PySparkMergeIntoWriter.whenNotMatchedBySource.__doc__

    def withSchemaEvolution(self) -> "MergeIntoWriter":
        self._schema_evolution_enabled = True
        return self

    withSchemaEvolution.__doc__ = PySparkMergeIntoWriter.withSchemaEvolution.__doc__

    def merge(self) -> None:
        def a2e(a: proto.MergeAction) -> proto.Expression:
            return proto.Expression(merge_action=a)

        merge = proto.MergeIntoTableCommand(
            target_table_name=self._target_table,
            source_table_plan=self._source_plan.plan(self._client),
            merge_condition=self._condition.to_plan(self._client),
            match_actions=[a2e(a) for a in self._matched_actions],
            not_matched_actions=[a2e(a) for a in self._not_matched_actions],
            not_matched_by_source_actions=[a2e(a) for a in self._not_matched_by_source_actions],
            with_schema_evolution=self._schema_evolution_enabled,
        )
        _, _, ei = self._client.execute_command(
            proto.Command(merge_into_table_command=merge), self._source_plan.observations
        )
        self._callback(ei)

    merge.__doc__ = PySparkMergeIntoWriter.merge.__doc__

    class WhenMatched:
        def __init__(self, writer: "MergeIntoWriter", condition: Optional[Column]):
            self.writer = writer
            self._condition = condition

        def updateAll(self) -> "MergeIntoWriter":
            action = _build_merge_action(
                self.writer._client, proto.MergeAction.ACTION_TYPE_UPDATE_STAR, self._condition
            )
            self.writer._matched_actions.append(action)
            return self.writer

        updateAll.__doc__ = PySparkMergeIntoWriter.WhenMatched.updateAll.__doc__

        def update(self, assignments: Dict[str, Column]) -> "MergeIntoWriter":
            action = _build_merge_action(
                self.writer._client,
                proto.MergeAction.ACTION_TYPE_UPDATE,
                self._condition,
                assignments,
            )
            self.writer._matched_actions.append(action)
            return self.writer

        update.__doc__ = PySparkMergeIntoWriter.WhenMatched.update.__doc__

        def delete(self) -> "MergeIntoWriter":
            action = _build_merge_action(
                self.writer._client, proto.MergeAction.ACTION_TYPE_DELETE, self._condition
            )
            self.writer._matched_actions.append(action)
            return self.writer

        delete.__doc__ = PySparkMergeIntoWriter.WhenMatched.delete.__doc__

    WhenMatched.__doc__ = PySparkMergeIntoWriter.WhenMatched.__doc__

    class WhenNotMatched:
        def __init__(self, writer: "MergeIntoWriter", condition: Optional[Column]):
            self.writer = writer
            self._condition = condition

        def insertAll(self) -> "MergeIntoWriter":
            action = _build_merge_action(
                self.writer._client, proto.MergeAction.ACTION_TYPE_INSERT_STAR, self._condition
            )
            self.writer._not_matched_actions.append(action)
            return self.writer

        insertAll.__doc__ = PySparkMergeIntoWriter.WhenNotMatched.insertAll.__doc__

        def insert(self, assignments: Dict[str, Column]) -> "MergeIntoWriter":
            action = _build_merge_action(
                self.writer._client,
                proto.MergeAction.ACTION_TYPE_INSERT,
                self._condition,
                assignments,
            )
            self.writer._not_matched_actions.append(action)
            return self.writer

        insert.__doc__ = PySparkMergeIntoWriter.WhenNotMatched.insert.__doc__

    WhenNotMatched.__doc__ = PySparkMergeIntoWriter.WhenNotMatched.__doc__

    class WhenNotMatchedBySource:
        def __init__(self, writer: "MergeIntoWriter", condition: Optional[Column]):
            self.writer = writer
            self._condition = condition

        def updateAll(self) -> "MergeIntoWriter":
            action = _build_merge_action(
                self.writer._client, proto.MergeAction.ACTION_TYPE_UPDATE_STAR, self._condition
            )
            self.writer._not_matched_by_source_actions.append(action)
            return self.writer

        updateAll.__doc__ = PySparkMergeIntoWriter.WhenNotMatchedBySource.updateAll.__doc__

        def update(self, assignments: Dict[str, Column]) -> "MergeIntoWriter":
            action = _build_merge_action(
                self.writer._client,
                proto.MergeAction.ACTION_TYPE_UPDATE,
                self._condition,
                assignments,
            )
            self.writer._not_matched_by_source_actions.append(action)
            return self.writer

        update.__doc__ = PySparkMergeIntoWriter.WhenNotMatchedBySource.update.__doc__

        def delete(self) -> "MergeIntoWriter":
            action = _build_merge_action(
                self.writer._client, proto.MergeAction.ACTION_TYPE_DELETE, self._condition
            )
            self.writer._not_matched_by_source_actions.append(action)
            return self.writer

        delete.__doc__ = PySparkMergeIntoWriter.WhenNotMatchedBySource.delete.__doc__

    WhenNotMatchedBySource.__doc__ = PySparkMergeIntoWriter.WhenNotMatchedBySource.__doc__


MergeIntoWriter.__doc__ = PySparkMergeIntoWriter.__doc__


def _test() -> None:
    import doctest
    import os
    from pyspark.sql import SparkSession as PySparkSession
    import pyspark.sql.connect.merge

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.connect.merge.__dict__.copy()
    globs["spark"] = (
        PySparkSession.builder.appName("sql.connect.dataframe tests")
        .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.merge,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF,
    )
    globs["spark"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()

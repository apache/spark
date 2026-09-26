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

from typing import TYPE_CHECKING, Any, final

from typing_extensions import Self

from pyspark.graphframes.classic.utils import storage_level_to_jvm
from pyspark.ml.wrapper import JavaWrapper
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.classic.column import _to_seq
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

if TYPE_CHECKING:
    from pyspark.graphframes.classic.graphframe import GraphFrame


@final
class Pregel(JavaWrapper):
    """Implements a Pregel-like bulk-synchronous message-passing API based on DataFrame operations.

    See `Malewicz et al., Pregel: a system for large-scale graph processing <https://doi.org/10.1145/1807167.1807184>`_
    for a detailed description of the Pregel algorithm.

    You can construct a Pregel instance using either this constructor or :attr:`graphframes.GraphFrame.pregel`,
    then use builder pattern to describe the operations, and then call :func:`run` to start a run.
    It returns a DataFrame of vertices from the last iteration.

    When a run starts, it expands the vertices DataFrame using column expressions defined by :func:`withVertexColumn`.
    Those additional vertex properties can be changed during Pregel iterations.
    In each Pregel iteration, there are three phases:

    - Given each edge triplet, generate messages and specify target vertices to send, described by :func:`sendMsgToDst` and :func:`sendMsgToSrc`.
    - Aggregate messages by target vertex IDs, described by :func:`aggMsgs`.
    - Update additional vertex properties based on aggregated messages and states from previous iteration, described by :func:`withVertexColumn`.

    Please find what columns you can reference at each phase in the method API docs.

    You can control the number of iterations by :func:`setMaxIter` and check API docs for advanced controls.

    :param graph: a :class:`graphframes.GraphFrame` object holding a graph with vertices and edges stored as DataFrames.
    """

    def __init__(self, graph: "GraphFrame") -> None:
        super(Pregel, self).__init__()

        self.graph = graph
        self._java_obj: Any = self._new_java_obj(
            "org.apache.spark.graphframes.lib.Pregel", graph._jvm_graph
        )

    def setMaxIter(self, value: int) -> "Pregel":
        """Sets the max number of iterations (default: 10).

        :param value: the number of Pregel iterations
        """
        self._java_obj.setMaxIter(int(value))
        return self

    def setCheckpointInterval(self, value: int) -> "Pregel":
        """Sets the number of iterations between two checkpoints (default: 2).

        This is an advanced control to balance query plan optimization and checkpoint data I/O cost.
        In most cases, you should keep the default value.

        Checkpoint is disabled if this is set to 0.
        """
        self._java_obj.setCheckpointInterval(int(value))
        return self

    def setEarlyStopping(self, value: bool) -> "Pregel":
        """Set should Pregel stop earlier in case of no new messages to send or not.

        Early stopping allows to terminate Pregel before reaching maxIter by checking if there are any non-null messages.
        While in some cases it may gain significant performance boost, in other cases it can lead to performance degradation,
        because checking if the messages DataFrame is empty or not is an action and requires materialization of the Spark Plan
        with some additional computations.

        In the case when the user can assume a good value of maxIter, it is recommended to leave this value to the default "false".
        In the case when it is hard to estimate the number of iterations required for convergence,
        it is recommended to set this value to "false" to avoid iterating over convergence until reaching maxIter.
        When this value is "true", maxIter can be set to a bigger value without risks.
        """
        self._java_obj.setEarlyStopping(bool(value))
        return self

    def withVertexColumn(
        self, colName: str, initialExpr: Column, updateAfterAggMsgsExpr: Column
    ) -> "Pregel":
        """Defines an additional vertex column at the start of run and how to update it in each iteration.

        You can call it multiple times to add more than one additional vertex columns.

        :param colName: the name of the additional vertex column.
                        It cannot be an existing vertex column in the graph.
        :param initialExpr: the expression to initialize the additional vertex column.
                            You can reference all original vertex columns in this expression.
        :param updateAfterAggMsgsExpr: the expression to update the additional vertex column after messages aggregation.
                                       You can reference all original vertex columns, additional vertex columns, and the
                                       aggregated message column using :func:`msg`.
                                       If the vertex received no messages, the message column would be null.
        """
        self._java_obj.withVertexColumn(colName, initialExpr._jc, updateAfterAggMsgsExpr._jc)
        return self

    def sendMsgToSrc(self, msgExpr: Column) -> "Pregel":
        """Defines a message to send to the source vertex of each edge triplet.

        You can call it multiple times to send more than one messages.

        See method :func:`sendMsgToDst`.

        :param msgExpr: the expression of the message to send to the source vertex given a (src, edge, dst) triplet.
                        Source/destination vertex properties and edge properties are nested under columns `src`, `dst`,
                        and `edge`, respectively.
                        You can reference them using :func:`src`, :func:`dst`, and :func:`edge`.
                        Null messages are not included in message aggregation.
        """
        self._java_obj.sendMsgToSrc(msgExpr._jc)
        return self

    def sendMsgToDst(self, msgExpr: Column) -> "Pregel":
        """Defines a message to send to the destination vertex of each edge triplet.

        You can call it multiple times to send more than one messages.

        See method :func:`sendMsgToSrc`.

        :param msgExpr: the message expression to send to the destination vertex given a (`src`, `edge`, `dst`) triplet.
                        Source/destination vertex properties and edge properties are nested under columns `src`, `dst`,
                        and `edge`, respectively.
                        You can reference them using :func:`src`, :func:`dst`, and :func:`edge`.
                        Null messages are not included in message aggregation.
        """
        self._java_obj.sendMsgToDst(msgExpr._jc)
        return self

    def aggMsgs(self, aggExpr: Column) -> "Pregel":
        """Defines how messages are aggregated after grouped by target vertex IDs.

        :param aggExpr: the message aggregation expression, such as `sum(Pregel.msg())`.
                        You can reference the message column by :func:`msg` and the vertex ID by `col("id")`,
                        while the latter is usually not used.
        """
        self._java_obj.aggMsgs(aggExpr._jc)
        return self

    def setStopIfAllNonActiveVertices(self, value: bool) -> Self:
        """Set should Pregel stop if all the vertices voted to halt.

        Activity (or vote) is determined based on the activity_col.
        See methods :func:`setInitialActiveVertexExpression` and :func:`setUpdateActiveVertexExpression` for details
        how to set and update activity_col.

        Be aware that checking of the vote is not free but a Spark Action. In case the
        condition is not realistically reachable but set, it will just slow down the algorithm.

        :param value: the boolean value.
        """
        self._java_obj.setStopIfAllNonActiveVertices(value)
        return self

    def setInitialActiveVertexExpression(self, value: Column) -> Self:
        """Sets the initial expression for the active vertex column.

        The active vertex column is used to determine if a vertices voting result on each iteration of Pregel.
        This expression is evaluated on the initial vertices DataFrame to set the initial state of the activity column.

        :param value: expression to compute the initial active state of vertices.
                      You can reference all original vertex columns in this expression.
        """
        self._java_obj.setInitialActiveVertexExpression(value._jc)
        return self

    def setUpdateActiveVertexExpression(self, value: Column) -> Self:
        """Sets the expression to update the active vertex column.

        The active vertex column is used to determine if a vertices voting result on each iteration of Pregel.
        This expression is evaluated on the updated vertices DataFrame to set the new state of the activity column.

        :param value: expression to compute the new active state of vertices.
                      You can reference all original vertex columns and additional vertex columns in this expression.
        """
        self._java_obj.setUpdateActiveVertexExpression(value._jc)
        return self

    def setSkipMessagesFromNonActiveVertices(self, value: bool) -> Self:
        """Set should Pregel skip sending messages from non-active vertices.

        When this option is enabled, messages will not be sent from vertices that are marked as inactive.
        This can help optimize performance by avoiding unnecessary message propagation from inactive vertices.

        :param value: boolean value.
        """
        self._java_obj.setSkipMessagesFromNonActiveVertices(value)
        return self

    def setUseLocalCheckpoints(self, value: bool) -> Self:
        """Set should Pregel use local checkpoints.

        Local checkpoints are faster and do not require configuring a persistent storage.
        At the same time, local checkpoints are less reliable and may create a big load on local disks of executors.

        :param value: boolean value.
        """
        self._java_obj.setUseLocalCheckpoints(value)
        return self

    def setIntermediateStorageLevel(self, storage_level: StorageLevel) -> Self:
        """Set the intermediate storage level.
        On each iteration, Pregel cache results with a requested storage level.

        For very big graphs it is recommended to use DISK_ONLY.

        :param storage_level: storage level to use.
        """
        self._java_obj.setIntermediateStorageLevel(
            storage_level_to_jvm(storage_level, self.graph._spark)
        )
        return self

    def required_src_columns(self, col_name: str, *col_names: str) -> Self:
        """Specifies which source vertex columns are required when constructing triplets.

        By default, all source vertex columns are included in triplets, which can create large
        intermediate datasets for algorithms with significant state (e.g., cycle detection,
        random walks). Use this method to reduce memory usage by specifying only the columns
        that are actually needed by the sendMsgToSrc and sendMsgToDst expressions.

        The ID column and the active flag column (if used) are always included automatically.

        :param col_name: the first required source vertex column name
        :param col_names: additional required source vertex column names

        See also :func:`required_dst_columns`
        """
        self._java_obj.requiredSrcColumns(
            col_name, _to_seq(self.graph._spark.sparkContext, col_names)
        )
        return self

    def required_dst_columns(self, col_name: str, *col_names: str) -> Self:
        """Specifies which destination vertex columns are required when constructing triplets.

        By default, all destination vertex columns are included in triplets, which can create large
        intermediate datasets for algorithms with significant state (e.g., cycle detection,
        random walks). Use this method to reduce memory usage by specifying only the columns
        that are actually needed by the sendMsgToSrc and sendMsgToDst expressions.

        The ID column and the active flag column (if used) are always included automatically.

        :param col_name: the first required destination vertex column name
        :param col_names: additional required destination vertex column names

        See also :func:`required_src_columns`
        """
        self._java_obj.requiredDstColumns(
            col_name, _to_seq(self.graph._spark.sparkContext, col_names)
        )
        return self

    def required_edge_columns(self, col_name: str, *col_names: str) -> Self:
        """Specifies which edge columns are required when constructing triplets.

        By default, only src and dst columns are included from edges. Use this method to
        specify additional edge columns that are needed by the sendMsgToSrc and sendMsgToDst
        expressions.

        :param col_name: the first required edge column name
        :param col_names: additional required edge column names

        See also :func:`required_src_columns` and :func:`required_dst_columns`
        """
        self._java_obj.requiredEdgeColumns(
            col_name, _to_seq(self.graph._spark.sparkContext, col_names)
        )
        return self

    def run(self) -> DataFrame:
        """Runs the defined Pregel algorithm.

        :return: the result vertex DataFrame from the final iteration including both original and additional columns.
        """
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise ValueError("SparkSession is dead or did not started.")
        return DataFrame(self._java_obj.run(), spark)

    @staticmethod
    def msg() -> Column:
        """References the message column in aggregating messages and updating additional vertex columns.

        See :func:`aggMsgs` and :func:`withVertexColumn`
        """
        return col("_pregel_msg_")

    @staticmethod
    def src(colName: str) -> Column:
        """References a source vertex column in generating messages to send.

        See :func:`sendMsgToSrc` and :func:`sendMsgToDst`

        :param colName: the vertex column name.
        """
        return col("src." + colName)

    @staticmethod
    def dst(colName: str) -> Column:
        """
        References a destination vertex column in generating messages to send.

        See :func:`sendMsgToSrc` and :func:`sendMsgToDst`

        :param colName: the vertex column name.
        """
        return col("dst." + colName)

    @staticmethod
    def edge(colName: str) -> Column:
        """
        References an edge column in generating messages to send.

        See :func:`sendMsgToSrc` and :func:`sendMsgToDst`

        :param colName: the edge column name.
        """
        return col("edge." + colName)

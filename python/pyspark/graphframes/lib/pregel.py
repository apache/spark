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

from __future__ import annotations

from typing import Any

from typing_extensions import Self

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


class Pregel:
    """Mode-independent wrapper for the GraphFrames Pregel builder API."""

    def __init__(self, graph: Any) -> None:
        self.graph = graph
        graph_impl = getattr(graph, "_impl", graph)
        self._impl = graph_impl.pregel

    def setMaxIter(self, value: int) -> Self:
        self._impl.setMaxIter(value)
        return self

    def setCheckpointInterval(self, value: int) -> Self:
        self._impl.setCheckpointInterval(value)
        return self

    def setEarlyStopping(self, value: bool) -> Self:
        self._impl.setEarlyStopping(value)
        return self

    def withVertexColumn(
        self,
        colName: str,
        initialExpr: Column | str,
        updateAfterAggMsgsExpr: Column | str,
    ) -> Self:
        self._impl.withVertexColumn(colName, initialExpr, updateAfterAggMsgsExpr)
        return self

    def sendMsgToSrc(self, msgExpr: Column | str) -> Self:
        self._impl.sendMsgToSrc(msgExpr)
        return self

    def sendMsgToDst(self, msgExpr: Column | str) -> Self:
        self._impl.sendMsgToDst(msgExpr)
        return self

    def aggMsgs(self, aggExpr: Column) -> Self:
        self._impl.aggMsgs(aggExpr)
        return self

    def setStopIfAllNonActiveVertices(self, value: bool) -> Self:
        self._impl.setStopIfAllNonActiveVertices(value)
        return self

    def setInitialActiveVertexExpression(self, value: Column | str) -> Self:
        self._impl.setInitialActiveVertexExpression(value)
        return self

    def setUpdateActiveVertexExpression(self, value: Column | str) -> Self:
        self._impl.setUpdateActiveVertexExpression(value)
        return self

    def setSkipMessagesFromNonActiveVertices(self, value: bool) -> Self:
        self._impl.setSkipMessagesFromNonActiveVertices(value)
        return self

    def setUseLocalCheckpoints(self, value: bool) -> Self:
        self._impl.setUseLocalCheckpoints(value)
        return self

    def setIntermediateStorageLevel(self, storage_level: StorageLevel) -> Self:
        self._impl.setIntermediateStorageLevel(storage_level)
        return self

    def required_src_columns(self, col_name: str, *col_names: str) -> Self:
        self._impl.required_src_columns(col_name, *col_names)
        return self

    def required_dst_columns(self, col_name: str, *col_names: str) -> Self:
        self._impl.required_dst_columns(col_name, *col_names)
        return self

    def required_edge_columns(self, col_name: str, *col_names: str) -> Self:
        self._impl.required_edge_columns(col_name, *col_names)
        return self

    def run(self) -> DataFrame:
        return self._impl.run()

    @staticmethod
    def msg() -> Column:
        return F.col("_pregel_msg_")

    @staticmethod
    def src(colName: str) -> Column:
        return F.col("src." + colName)

    @staticmethod
    def dst(colName: str) -> Column:
        return F.col("dst." + colName)

    @staticmethod
    def edge(colName: str) -> Column:
        return F.col("edge." + colName)

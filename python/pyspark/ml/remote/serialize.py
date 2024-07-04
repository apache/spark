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
from typing import Any, List, TYPE_CHECKING, Mapping, Optional

import pyspark.sql.connect.proto as pb2
from pyspark.ml.linalg import (
    Vectors,
    Matrices,
    DenseVector,
    SparseVector,
    DenseMatrix,
    SparseMatrix,
)
from pyspark.sql.connect.dataframe import DataFrame as RemoteDataFrame
from pyspark.sql.connect.expressions import LiteralExpression

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.ml.param import Params


def serialize_param(value: Any, client: "SparkConnectClient") -> pb2.Param:
    if isinstance(value, DenseVector):
        return pb2.Param(vector=pb2.Vector(dense=pb2.Vector.Dense(value=value.values.tolist())))
    elif isinstance(value, SparseVector):
        return pb2.Param(
            vector=pb2.Vector(
                sparse=pb2.Vector.Sparse(
                    size=value.size, index=value.indices.tolist(), value=value.values.tolist()
                )
            )
        )
    elif isinstance(value, DenseMatrix):
        return pb2.Param(
            matrix=pb2.Matrix(
                dense=pb2.Matrix.Dense(
                    num_rows=value.numRows, num_cols=value.numCols, value=value.values.tolist()
                )
            )
        )
    elif isinstance(value, SparseMatrix):
        return pb2.Param(
            matrix=pb2.Matrix(
                sparse=pb2.Matrix.Sparse(
                    num_rows=value.numRows,
                    num_cols=value.numCols,
                    colptr=value.colPtrs.tolist(),
                    row_index=value.rowIndices.tolist(),
                    value=value.values.tolist(),
                )
            )
        )
    else:
        literal = LiteralExpression._from_value(value).to_plan(client).literal
        return pb2.Param(literal=literal)


def serialize(client: "SparkConnectClient", *args: Any) -> List[Any]:
    result = []
    for arg in args:
        if isinstance(arg, RemoteDataFrame):
            result.append(pb2.FetchModelAttr.Args(input=arg._plan.plan(client)))
        else:
            result.append(pb2.FetchModelAttr.Args(param=serialize_param(arg, client)))
    return result


def deserialize_param(param: pb2.Param) -> Any:
    if param.HasField("literal"):
        return LiteralExpression._to_value(param.literal)
    if param.HasField("vector"):
        vector = param.vector
        # TODO support sparse vector
        if vector.HasField("dense"):
            return Vectors.dense(vector.dense.value)
        raise ValueError("TODO, support sparse vector")

    if param.HasField("matrix"):
        matrix = param.matrix
        # TODO support sparse matrix
        if matrix.HasField("dense") and not matrix.dense.is_transposed:
            return Matrices.dense(
                matrix.dense.num_rows,
                matrix.dense.num_cols,
                matrix.dense.value,
            )
        raise ValueError("TODO, support sparse matrix")

    raise ValueError("Unsupported param type")


def deserialize(ml_command_result: Optional[pb2.MlCommandResponse]) -> Any:
    assert ml_command_result is not None
    if ml_command_result.HasField("operator_info"):
        return ml_command_result.operator_info

    if ml_command_result.HasField("param"):
        return deserialize_param(ml_command_result.param)
    raise ValueError()


def serialize_ml_params(instance: "Params", client: "SparkConnectClient") -> pb2.MlParams:
    params: Mapping[str, pb2.Param] = {
        k.name: serialize_param(v, client) for k, v in instance._paramMap.items()
    }
    return pb2.MlParams(params=params)

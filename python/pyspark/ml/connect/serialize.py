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
from typing import Any, List, TYPE_CHECKING, Mapping, Dict

import pyspark.sql.connect.proto as pb2
from pyspark.ml.linalg import (
    VectorUDT,
    MatrixUDT,
    DenseVector,
    SparseVector,
    DenseMatrix,
    SparseMatrix,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.ml.param import Params


def literal_null() -> pb2.Expression.Literal:
    dt = pb2.DataType()
    dt.null.CopyFrom(pb2.DataType.NULL())
    return pb2.Expression.Literal(null=dt)


def build_int_list(value: List[int]) -> pb2.Expression.Literal:
    p = pb2.Expression.Literal()
    p.specialized_array.ints.values.extend(value)
    return p


def build_float_list(value: List[float]) -> pb2.Expression.Literal:
    p = pb2.Expression.Literal()
    p.specialized_array.doubles.values.extend(value)
    return p


def serialize_param(value: Any, client: "SparkConnectClient") -> pb2.Expression.Literal:
    from pyspark.sql.connect.types import pyspark_types_to_proto_types
    from pyspark.sql.connect.expressions import LiteralExpression

    if isinstance(value, SparseVector):
        p = pb2.Expression.Literal()
        p.struct.struct_type.CopyFrom(pyspark_types_to_proto_types(VectorUDT.sqlType()))
        # type = 0
        p.struct.elements.append(pb2.Expression.Literal(byte=0))
        # size
        p.struct.elements.append(pb2.Expression.Literal(integer=value.size))
        # indices
        p.struct.elements.append(build_int_list(value.indices.tolist()))
        # values
        p.struct.elements.append(build_float_list(value.values.tolist()))
        return p

    elif isinstance(value, DenseVector):
        p = pb2.Expression.Literal()
        p.struct.struct_type.CopyFrom(pyspark_types_to_proto_types(VectorUDT.sqlType()))
        # type = 1
        p.struct.elements.append(pb2.Expression.Literal(byte=1))
        # size = null
        p.struct.elements.append(literal_null())
        # indices = null
        p.struct.elements.append(literal_null())
        # values
        p.struct.elements.append(build_float_list(value.values.tolist()))
        return p

    elif isinstance(value, SparseMatrix):
        p = pb2.Expression.Literal()
        p.struct.struct_type.CopyFrom(pyspark_types_to_proto_types(MatrixUDT.sqlType()))
        # type = 0
        p.struct.elements.append(pb2.Expression.Literal(byte=0))
        # numRows
        p.struct.elements.append(pb2.Expression.Literal(integer=value.numRows))
        # numCols
        p.struct.elements.append(pb2.Expression.Literal(integer=value.numCols))
        # colPtrs
        p.struct.elements.append(build_int_list(value.colPtrs.tolist()))
        # rowIndices
        p.struct.elements.append(build_int_list(value.rowIndices.tolist()))
        # values
        p.struct.elements.append(build_float_list(value.values.tolist()))
        # isTransposed
        p.struct.elements.append(pb2.Expression.Literal(boolean=value.isTransposed))
        return p

    elif isinstance(value, DenseMatrix):
        p = pb2.Expression.Literal()
        p.struct.struct_type.CopyFrom(pyspark_types_to_proto_types(MatrixUDT.sqlType()))
        # type = 1
        p.struct.elements.append(pb2.Expression.Literal(byte=1))
        # numRows
        p.struct.elements.append(pb2.Expression.Literal(integer=value.numRows))
        # numCols
        p.struct.elements.append(pb2.Expression.Literal(integer=value.numCols))
        # colPtrs = null
        p.struct.elements.append(literal_null())
        # rowIndices = null
        p.struct.elements.append(literal_null())
        # values
        p.struct.elements.append(build_float_list(value.values.tolist()))
        # isTransposed
        p.struct.elements.append(pb2.Expression.Literal(boolean=value.isTransposed))
        return p

    else:
        return LiteralExpression._from_value(value).to_plan(client).literal


def serialize(client: "SparkConnectClient", *args: Any) -> List[Any]:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

    result = []
    for arg in args:
        if isinstance(arg, ConnectDataFrame):
            result.append(pb2.Fetch.Method.Args(input=arg._plan.plan(client)))
        else:
            result.append(pb2.Fetch.Method.Args(param=serialize_param(arg, client)))
    return result


def deserialize_param(literal: pb2.Expression.Literal) -> Any:
    from pyspark.sql.connect.types import proto_schema_to_pyspark_data_type
    from pyspark.sql.connect.expressions import LiteralExpression

    if literal.HasField("struct"):
        s = literal.struct
        schema = proto_schema_to_pyspark_data_type(s.struct_type)

        if schema == VectorUDT.sqlType():
            assert len(s.elements) == 4
            tpe = s.elements[0].byte
            if tpe == 0:
                size = s.elements[1].integer
                indices = s.elements[2].specialized_array.ints.values
                values = s.elements[3].specialized_array.doubles.values
                return SparseVector(size, indices, values)
            elif tpe == 1:
                values = s.elements[3].specialized_array.doubles.values
                return DenseVector(values)
            else:
                raise ValueError(f"Unknown Vector type {tpe}")

        elif schema == MatrixUDT.sqlType():
            assert len(s.elements) == 7
            tpe = s.elements[0].byte
            if tpe == 0:
                numRows = s.elements[1].integer
                numCols = s.elements[2].integer
                colPtrs = s.elements[3].specialized_array.ints.values
                rowIndices = s.elements[4].specialized_array.ints.values
                values = s.elements[5].specialized_array.doubles.values
                isTransposed = s.elements[6].boolean
                return SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
            elif tpe == 1:
                numRows = s.elements[1].integer
                numCols = s.elements[2].integer
                values = s.elements[5].specialized_array.doubles.values
                isTransposed = s.elements[6].boolean
                return DenseMatrix(numRows, numCols, values, isTransposed)
            else:
                raise ValueError(f"Unknown Matrix type {tpe}")
        else:
            raise ValueError(f"Unsupported parameter struct {schema}")
    else:
        return LiteralExpression._to_value(literal)


def deserialize(ml_command_result_properties: Dict[str, Any]) -> Any:
    ml_command_result = ml_command_result_properties["ml_command_result"]
    if ml_command_result.HasField("operator_info"):
        return ml_command_result.operator_info

    if ml_command_result.HasField("param"):
        return deserialize_param(ml_command_result.param)

    raise ValueError("Unsupported result type")


def serialize_ml_params(instance: "Params", client: "SparkConnectClient") -> pb2.MlParams:
    params: Mapping[str, pb2.Expression.Literal] = {
        k.name: serialize_param(v, client) for k, v in instance._paramMap.items()
    }
    return pb2.MlParams(params=params)

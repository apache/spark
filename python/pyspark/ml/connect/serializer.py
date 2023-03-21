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

import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.ml_pb2 as ml_pb2
import pyspark.sql.connect.proto.ml_common_pb2 as ml_common_pb2

from pyspark.sql.connect.expressions import LiteralExpression

from pyspark.ml.linalg import Vector, Vectors, Matrices, Matrix


def deserialize_response_value(ml_command_result: ml_pb2.MlCommandResponse, client, **kwargs):
    from pyspark.ml.connect.base import ModelRef

    if ml_command_result.HasField("literal"):
        return LiteralExpression._to_value(ml_command_result.literal)

    if ml_command_result.HasField("model_info"):
        model_info = ml_command_result.model_info
        assert "clazz" in kwargs
        clazz = kwargs["clazz"]

        model = clazz()
        model._resetUid(model_info.model_uid)
        set_instance_params_from_proto(model, model_info.params)
        model.model_ref = ModelRef.from_proto(model_info.model_ref)
        return model

    if ml_command_result.HasField("model_ref"):
        return ModelRef.from_proto(ml_command_result.model_ref)

    if ml_command_result.HasField("vector"):
        vector_pb = ml_command_result.vector
        return deserialize_vector(vector_pb)

    if ml_command_result.HasField("matrix"):
        matrix_pb = ml_command_result.matrix
        return deserialize_matrix(matrix_pb)

    if ml_command_result.HasField("stage"):
        assert "clazz" in kwargs
        clazz = kwargs["clazz"]
        stage_pb = ml_command_result.stage
        stage = clazz()
        stage._resetUid(stage_pb.uid)
        set_instance_params_from_proto(stage, stage_pb.params)
        return stage

    raise ValueError()


def deserialize_vector(vector_pb):
    # TODO: support sparse
    # TODO: support large vector
    if vector_pb.HasField("dense"):
        return Vectors.dense(vector_pb.dense.value)
    raise ValueError()


def serialize_vector(vector):
    return ml_common_pb2.Vector(
        dense=ml_common_pb2.Vector.Dense(
            value=vector.toArray()
        )
    )


def deserialize_matrix(matrix_pb):
    # TODO: support sparse, is_transposed
    # TODO: support large matrix
    if matrix_pb.HasField("dense") and not matrix_pb.dense.is_transposed:
        return Matrices.dense(
            matrix_pb.dense.num_rows,
            matrix_pb.dense.num_cols,
            matrix_pb.dense.value,
        )
    raise ValueError()


def serialize_matrix(matrix):
    # TODO: support sparse, is_transposed
    # TODO: support large matrix
    return ml_common_pb2.Matrix(
        dense=ml_common_pb2.Matrix.Dense(
            num_rows=matrix.numRows,
            num_cols=matrix.numCols,
            value=matrix.toArray(),
            is_transposed=False
        )
    )


def deserialize_param_value(value_pb: ml_common_pb2.MlParams.ParamValue):
    if value_pb.HasField("literal"):
        return LiteralExpression._to_value(value_pb.literal)
    if value_pb.HasField("vector"):
        return deserialize_vector(value_pb.vector)
    if value_pb.HasField("matrix"):
        return deserialize_vector(value_pb.matrix)


def serialize_param_value(value, client):
    if isinstance(value, Vector):
        return ml_common_pb2.MlParams.ParamValue(
            vector= serialize_vector(value)
        )
    if isinstance(value, Matrix):
        return ml_common_pb2.MlParams.ParamValue(
            matrix=serialize_matrix(value)
        )
    return ml_common_pb2.MlParams.ParamValue(
        literal=LiteralExpression._from_value(value).to_plan(client).literal
    )


def set_instance_params_from_proto(instance, params_proto):
    instance._set(**{
        k: deserialize_param_value(v_pb)
        for k, v_pb in params_proto.params.items()
    })
    instance._setDefault(**{
        k: deserialize_param_value(v_pb)
        for k, v_pb in params_proto.params.items()
    })


def serialize_ml_params(instance, client):
    def gen_pb2_map(param_value_dict):
        return {
            k.name: serialize_param_value(v, client)
            for k, v in param_value_dict.items()
        }

    result = ml_common_pb2.MlParams(
        params=gen_pb2_map(instance._paramMap),
        default_params=gen_pb2_map(instance._defaultParamMap),
    )
    return result


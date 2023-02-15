from typing import Type
import pyspark.sql.connect.proto as proto
from collections import namedtuple
from pyspark.sql.connect.dataframe import DataFrame
import numpy as np
from pyspark.ml.linalg import Vectors, Matrices


_spark_client_mode_enabled = True


def set_spark_client_mode(enabled):
    _spark_client_mode_enabled = enabled


def is_spark_client_mode():
    return _spark_client_mode_enabled


class RemoteObject:

    def __init__(self, object_id):
        self.object_id = object_id

    def __del__(self):
        # TODO:
        #  send request to server to delete server side object
        pass

    def to_proto(self):
        return proto.RemoteCall.RemoteObject(id=self.object_id)


class RemoteDataFramePlan:
    def __init__(self, remote_java_object):
        # Note: remote_java_object contains `__del__` that
        # can trigger server side GC work when this object is no longer used.
        self.remote_java_object = remote_java_object

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        return proto.Relation(server_side_dataframe_id=self.remote_java_object.object_id)


def _serialize_arg(arg_value, session):
    if isinstance(arg_value, bool):
        # Note: bool type must be put before int type.
        return proto.RemoteCall.ArgValue(bool_value=arg_value)
    if isinstance(arg_value, int):
        return proto.RemoteCall.ArgValue(int64_value=arg_value)
    if isinstance(arg_value, float):
        return proto.RemoteCall.ArgValue(double_value=arg_value)
    if isinstance(arg_value, str):
        return proto.RemoteCall.ArgValue(string_value=arg_value)
    if isinstance(arg_value, list):
        proto_list = proto.RemoteCall.List()
        for elem in arg_value:
            proto_elem = _serialize_arg(elem, session)
            proto_list.element.append(proto_elem)
        return proto.RemoteCall.ArgValue(list=proto_list)
    if isinstance(arg_value, dict):
        proto_map = proto.RemoteCall.Map()
        for key, value in arg_value.items():
            proto_value = _serialize_arg(value, session)
            proto_map.map[key].CopyFrom(proto_value)
        return proto.RemoteCall.ArgValue(map=proto_map)
    if isinstance(arg_value, RemoteObject):
        proto_remote_obj = proto.RemoteCall.RemoteObject(id=arg_value.object_id)
        return proto.RemoteCall.ArgValue(remote_object=proto_remote_obj)
    if isinstance(arg_value, DataFrame):
        df_plan = arg_value._plan.plan(session.client)
        proto_arg_value = proto.RemoteCall.ArgValue()
        proto_arg_value.relation.CopyFrom(df_plan)
        return proto_arg_value

    raise ValueError("Unsupported argument type.")


def _deserialize_return_value(resp: "proto.ExecutePlanResponse", session):
    if not resp.HasField("remote_call_return_value"):
        return None

    proto_return_value = resp.remote_call_return_value

    if proto_return_value.HasField("int32_value"):
        return proto_return_value.int32_value

    if proto_return_value.HasField("int64_value"):
        return proto_return_value.int64_value

    if proto_return_value.HasField("float_value"):
        return proto_return_value.float_value

    if proto_return_value.HasField("double_value"):
        return proto_return_value.double_value

    if proto_return_value.HasField("bool_value"):
        return proto_return_value.bool_value

    if proto_return_value.HasField("string_value"):
        return proto_return_value.string_value

    if proto_return_value.HasField("remote_object"):
        remote_obj = RemoteObject(proto_return_value.remote_object.id)
        if proto_return_value.remote_object.class_name == "org.apache.spark.sql.Dataset":
            return _create_remote_dataframe(remote_obj, session)
        return remote_obj

    if proto_return_value.HasField("vector"):
        return Vectors.dense(proto_return_value.vector.element)

    if proto_return_value.HasField("matrix"):
        return Matrices.dense(
            proto_return_value.matrix.num_rows,
            proto_return_value.matrix.num_cols,
            proto_return_value.matrix.values
        )

    # TODO: support other return value types
    raise RuntimeError()


def invoke_remote_method(remote_object: "RemoteObject", method_name, arg_value_list, session):
    proto_call_method = proto.RemoteCall.CallMethod(
        remote_object=remote_object.to_proto(),
        method_name=method_name,
    )
    for arg_value in arg_value_list:
        proto_call_method.arg_values.append(_serialize_arg(arg_value, session))

    client = session.client
    req = client._execute_plan_request_with_metadata()
    req.plan.remote_call.CopyFrom(proto.RemoteCall(call_method=proto_call_method))
    resp = client._execute_ml(req)
    return _deserialize_return_value(resp, session)


def invoke_remote_function(module_name, function_name, arg_value_list, session):
    proto_call_function = proto.RemoteCall.CallFunction(
        module_name=module_name,
        function_name=function_name,
    )
    for arg_value in arg_value_list:
        proto_call_function.arg_values.append(_serialize_arg(arg_value, session))

    client = session.client
    req = client._execute_plan_request_with_metadata()
    req.plan.remote_call.CopyFrom(proto.RemoteCall(call_function=proto_call_function))
    resp = client._execute_ml(req)
    return _deserialize_return_value(resp, session)


def construct_remote_object(class_name, arg_value_list, session):
    proto_construct_object = proto.RemoteCall.ConstructObject(
        class_name=class_name,
    )
    for arg_value in arg_value_list:
        proto_construct_object.arg_values.append(_serialize_arg(arg_value, session))

    client = session.client
    req = client._execute_plan_request_with_metadata()
    req.plan.remote_call.CopyFrom(proto.RemoteCall(construct_object=proto_construct_object))
    resp = client._execute_ml(req)
    return _deserialize_return_value(resp, session)


def _create_remote_dataframe(remote_java_object, spark_session):
    plan = RemoteDataFramePlan(remote_java_object)
    return DataFrame.withPlan(plan, spark_session)

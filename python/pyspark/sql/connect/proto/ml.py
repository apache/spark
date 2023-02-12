from typing import Type
import pyspark.sql.connect.proto as proto
from collections import namedtuple
from pyspark.sql.connect.dataframe import DataFrame


class RemoteJavaObject:

    def __init__(self, object_id):
        self.object_id = object_id

    def __del__(self):
        # TODO:
        #  send request to server to delete server side object
        pass


class RemoteDataFramePlan:
    def __init__(self, remote_java_object):
        # Note: remote_java_object contains `__del__` that
        # can trigger server side GC work when this object is no longer used.
        self.remote_java_object = remote_java_object

    def plan(self, session: "SparkConnectClient") -> proto.Relation:
        return proto.Relation(server_side_dataframe_id=self.remote_java_object.object_id)


def _create_remote_dataframe(remote_java_object, spark_session):
    plan = RemoteDataFramePlan(remote_java_object)
    return DataFrame.withPlan(plan, spark_session)


def _parse_ml_response(ml_command_response: Type[proto.ExecutePlanResponse.MlCommandResponse]):
    if ml_command_response.HasField("server_side_object_id"):
        return RemoteJavaObject(ml_command_response.server_side_object_id)
    if ml_command_response.HasField("params"):
        # TODO.
        return


def _create_stage(spark_session, class_name, uid):
    client = spark_session.client
    req = client._execute_plan_request_with_metadata()
    ml_command = proto.MlCommand()
    ml_command.construct_stage.CopyFrom(proto.MlCommand.ConstructStage(
        uid=uid,
        class_name=class_name
    ))
    req.plan.ml_command.CopyFrom(ml_command)
    ml_resp = spark_session.client._execute_ml(req)
    return _parse_ml_response(ml_resp)


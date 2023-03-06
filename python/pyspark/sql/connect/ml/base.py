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

from pyspark.sql import DataFrame
from pyspark.ml import Estimator, Model
import import pyspark.sql.connect.proto as pb2
import pyspark.sql.connect.proto.ml_pb2 as ml_pb2
import pyspark.sql.connect.proto.ml_common_pb2 as ml_common_pb2
from pyspark.sql.connect.ml.serializer import deserialize
from pyspark.sql import SparkSession


class ClientEstimator(Estimator):

    def _algo_name(self): str

    def _create_model(self, model_ref_id, model_uid): Model

    def _fit(self, dataset: DataFrame) -> Model:
        client = dataset.sparkSession.client
        dataset_proto = dataset._plan.to_proto(client)
        estimator_proto = ml_common_pb2.Stage(
            name=self._algo_name(),
            # TODO: fill params
            params=ml_common_pb2.Params(params={}, default_params={}),
            uid=self.uid
        )
        fit_command_proto = ml_pb2.MlCommand.Fit(
            estimator=estimator_proto,
            dataset=dataset_proto
        )
        req = client._execute_plan_request_with_metadata()
        req.plan.ml_command.fit.CopyFrom(fit_command_proto)

        resp = client._execute_ml(req)
        model_ref_id, model_uid = deserialize(resp, client)
        model = self._create_model()
        model._resetUid(model_uid)
        model.ref_id = model_ref_id
        return model


class _ModelTransformRelationPlan:
    def __init__(self, model, dataset, client):
        # Note: remote_java_object contains `__del__` that
        # can trigger server side GC work when this object is no longer used.
        self.model_ref_id = model.ref_id
        self.dataset_proto = dataset._plan.to_proto(client)
        # TODO: fill params
        self.params_proto = ml_common_pb2.Params(params={}, default_params={})

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        return pb2.Relation(
            ml_relation=pb2.MlRelation(
                model_transform=pb2.MlRelation.ModelTransform(
                    input=self.dataset_proto,
                    model_ref_id=self.model_ref_id,
                    params=self.params_proto,
                )
            )
        )


class ClientModel(Model):

    ref_id: str
    uid: str

    def _get_model_attr(self, name):
        client = SparkSession.getActiveSession().client
        model_attr_command_proto = ml_pb2.MlCommand.ModelAttr(
            model_ref_id=self.model_ref_id,
            name=name
        )
        req = client._execute_plan_request_with_metadata()
        req.plan.ml_command.model_attr.CopyFrom(model_attr_command_proto)

        resp = client._execute_ml(req)
        return deserialize(resp, client)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        session = dataset.sparkSession
        plan = _ModelTransformRelationPlan(self, dataset)
        return DataFrame.withPlan(plan, session)

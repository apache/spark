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
import pyspark.sql.connect.proto as pb2
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
    def __init__(self, model, dataset):
        self.model = model
        self.dataset = dataset

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        dataset_proto = self.dataset._plan.to_proto(session.client)
        # TODO: fill params
        params_proto = ml_common_pb2.Params(params={}, default_params={})

        return pb2.Relation(
            ml_relation=pb2.MlRelation(
                model_transform=pb2.MlRelation.ModelTransform(
                    input=dataset_proto,
                    model_ref_id=self.model.ref_id,
                    params=params_proto,
                )
            )
        )


class _ModelAttrRelationPlan:
    def __init__(self, model, name):
        self.model = model
        self.name = name

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        return pb2.Relation(
            ml_relation=pb2.MlRelation(
                model_transform=pb2.MlRelation.ModelAttr(
                    model_ref_id=self.model.ref_id,
                    name=self.name
                )
            )
        )


class _ModelSummaryAttrRelationPlan:
    def __init__(self, model, name, dataset):
        self.model = model
        self.name = name
        self.dataset = dataset

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        if self.dataset is not None:
            dataset_proto = self.dataset._plan.to_proto(session.client)
        else:
            dataset_proto = None
        # TODO: fill params
        params_proto = ml_common_pb2.Params(params={}, default_params={})

        return pb2.Relation(
            ml_relation=pb2.MlRelation(
                model_summary_attr=pb2.MlRelation.ModelSummaryAttr(
                    model_ref_id=self.model.ref_id,
                    name=self.name,
                    params=params_proto,
                    evaluation_dataset=dataset_proto
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

    def _get_model_attr_dataframe(self, name) -> DataFrame:
        session = SparkSession.getActiveSession()
        plan = _ModelAttrRelationPlan(
            self.model, name
        )
        return DataFrame.withPlan(plan, session)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        session = dataset.sparkSession
        plan = _ModelTransformRelationPlan(self, dataset)
        return DataFrame.withPlan(plan, session)


class ClientModelSummary:
    def __init__(self, model, dataset):
        self.model = model
        self.dataset = dataset

    def _get_summary_attr_dataframe(self, name):
        session = SparkSession.getActiveSession()
        plan = _ModelSummaryAttrRelationPlan(
            self.model, name, dataset=None
        )
        return DataFrame.withPlan(plan, session)

    def _get_summary_attr(self, name):
        client = SparkSession.getActiveSession().client

        model_summary_attr_command_proto = ml_pb2.MlCommand.ModelSummaryAttr(
            model_ref_id=self.model.ref_id,
            name=name,
            # TODO: fill params
            params=ml_common_pb2.Params(params={}, default_params={}),
            evaluation_dataset=self.dataset._plan.to_proto(client)
        )
        req = client._execute_plan_request_with_metadata()
        req.plan.ml_command.model_summary_attr.CopyFrom(model_summary_attr_command_proto)

        resp = client._execute_ml(req)
        return deserialize(resp, client)


class HasTrainingSummary(Model):

    def hasSummary(self) -> bool:
        return self._get_model_attr("hasSummary")

    def summary(self):
        return ClientModelSummary(self, dataset=None)

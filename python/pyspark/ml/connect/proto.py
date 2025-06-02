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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)

from typing import Optional, TYPE_CHECKING, List

import pyspark.sql.connect.proto as pb2
from pyspark.sql.connect.plan import LogicalPlan

if TYPE_CHECKING:
    from pyspark.sql.connect.client import SparkConnectClient


class TransformerRelation(LogicalPlan):
    """A logical plan for transforming of a transformer which could be a cached model
    or a non-model transformer like VectorAssembler."""

    def __init__(
        self,
        child: Optional["LogicalPlan"],
        name: str,
        ml_params: pb2.MlParams,
        uid: str = "",
        is_model: bool = True,
    ) -> None:
        super().__init__(child)
        self._name = name
        self._ml_params = ml_params
        self._uid = uid
        self._is_model = is_model

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        assert self._child is not None
        plan = self._create_proto_relation()
        plan.ml_relation.transform.input.CopyFrom(self._child.plan(session))

        if self._is_model:
            plan.ml_relation.transform.obj_ref.CopyFrom(pb2.ObjectRef(id=self._name))
        else:
            plan.ml_relation.transform.transformer.CopyFrom(
                pb2.MlOperator(
                    name=self._name, uid=self._uid, type=pb2.MlOperator.OPERATOR_TYPE_TRANSFORMER
                )
            )

        if self._ml_params is not None:
            plan.ml_relation.transform.params.CopyFrom(self._ml_params)

        return plan


class AttributeRelation(LogicalPlan):
    """A logical plan used in ML to represent an attribute of an instance, which
    could be a model or a summary. This attribute returns a DataFrame.
    """

    def __init__(self, ref_id: str, methods: List[pb2.Fetch.Method]) -> None:
        super().__init__(None)
        self._ref_id = ref_id
        self._methods = methods

    def plan(self, session: "SparkConnectClient") -> pb2.Relation:
        plan = self._create_proto_relation()
        plan.ml_relation.fetch.obj_ref.CopyFrom(pb2.ObjectRef(id=self._ref_id))
        plan.ml_relation.fetch.methods.extend(self._methods)
        return plan

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

from pyspark.sql import SchemaRDD, inherit_doc
from pyspark.ml import JavaEstimator, Transformer, _jvm
from pyspark.ml.param.shared import HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,\
    HasRegParam


@inherit_doc
class LogisticRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                         HasRegParam):
    """
    Logistic regression.
    """

    def __init__(self):
        super(LogisticRegression, self).__init__()

    @property
    def _java_class(self):
        return "org.apache.spark.ml.classification.LogisticRegression"

    def _create_model(self, java_model):
        return LogisticRegressionModel(java_model)


@inherit_doc
class LogisticRegressionModel(Transformer):
    """
    Model fitted by LogisticRegression.
    """

    def __init__(self, java_model):
        self._java_model = java_model

    def transform(self, dataset, params={}):
        # TODO: handle params here.
        return SchemaRDD(self._java_model.transform(
            dataset._jschema_rdd,
            _jvm().org.apache.spark.ml.param.ParamMap()), dataset.sql_ctx)

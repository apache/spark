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
from pyspark.ml import Estimator, Transformer, _jvm
from pyspark.ml.param.shared import HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,\
    HasRegParam


@inherit_doc
class LogisticRegression(Estimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                         HasRegParam):
    """
    Logistic regression.
    """

    # _java_class = "org.apache.spark.ml.classification.LogisticRegression"

    def __init__(self):
        super(LogisticRegression, self).__init__()
        self._java_obj = _jvm().org.apache.spark.ml.classification.LogisticRegression()

    def fit(self, dataset, params=None):
        """
        Fits a dataset with optional parameters.
        """
        java_model = self._java_obj.fit(dataset._jschema_rdd,
                                        _jvm().org.apache.spark.ml.param.ParamMap())
        return LogisticRegressionModel(java_model)


class LogisticRegressionModel(Transformer):
    """
    Model fitted by LogisticRegression.
    """

    def __init__(self, _java_model):
        self._java_model = _java_model

    def transform(self, dataset):
        return SchemaRDD(self._java_model.transform(
            dataset._jschema_rdd,
            _jvm().org.apache.spark.ml.param.ParamMap()), dataset.sql_ctx)

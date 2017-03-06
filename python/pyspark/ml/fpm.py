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

from pyspark import keyword_only
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import *

__all__ = ["FPGrowth", "FPGrowthModel"]


class FPGrowthModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    .. versionadded:: 2.2.0
    """
    @property
    @since("2.2.0")
    def freqItemsets(self):
        """
        """
        return self._call_java("freqItemsets")

    @property
    @since("2.2.0")
    def associationRules(self):
        """
        """
        return self._call_java("associationRules")


class FPGrowth(JavaEstimator, HasFeaturesCol, HasPredictionCol,
               HasSupport, HasConfidence, JavaMLWritable, JavaMLReadable):
    """
    A parallel FP-growth algorithm to mine frequent itemsets.

    .. versionadded:: 2.2.0
    """
    @keyword_only
    def __init__(self, minSupport=0.3, minConfidence=0.8, featuresCol="features",
                 predictionCol="prediction", numPartitions=None):
        """
        """
        super(FPGrowth, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.fpm.FPGrowth", self.uid)
        self._setDefault(minSupport=0.3, minConfidence=0.8,
                         featuresCol="features", predictionCol="prediction")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.2.0")
    def setParams(self, minSupport=0.3, minConfidence=0.8, featuresCol="features",
                  predictionCol="prediction", numPartitions=None):
        """
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return FPGrowthModel(java_model)

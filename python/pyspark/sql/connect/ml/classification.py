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

from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
    TYPE_CHECKING,
)

from pyspark.sql import DataFrame
from pyspark.ml.classification import (
    ProbabilisticClassifier,
    _LogisticRegressionParams,
    _LogisticRegressionParamSetter,
    ProbabilisticClassificationModel
)
from pyspark.ml.common import inherit_doc
from pyspark.ml.linalg import (Matrix, Vector)
from pyspark.ml.util import HasTrainingSummary
from pyspark import keyword_only, since, SparkContext, inheritable_thread_target
from pyspark.sql.connect.ml.base import ClientEstimator

@inherit_doc
class LogisticRegression(
    ClientEstimator,
    ProbabilisticClassifier,
    _LogisticRegressionParamSetter,
):
    _input_kwargs: Dict[str, Any]

    @overload
    def __init__(
            self,
            *,
            featuresCol: str = ...,
            labelCol: str = ...,
            predictionCol: str = ...,
            maxIter: int = ...,
            regParam: float = ...,
            elasticNetParam: float = ...,
            tol: float = ...,
            fitIntercept: bool = ...,
            threshold: float = ...,
            probabilityCol: str = ...,
            rawPredictionCol: str = ...,
            standardization: bool = ...,
            weightCol: Optional[str] = ...,
            aggregationDepth: int = ...,
            family: str = ...,
            lowerBoundsOnCoefficients: Optional[Matrix] = ...,
            upperBoundsOnCoefficients: Optional[Matrix] = ...,
            lowerBoundsOnIntercepts: Optional[Vector] = ...,
            upperBoundsOnIntercepts: Optional[Vector] = ...,
            maxBlockSizeInMB: float = ...,
    ):
        ...

    @overload
    def __init__(
            self,
            *,
            featuresCol: str = ...,
            labelCol: str = ...,
            predictionCol: str = ...,
            maxIter: int = ...,
            regParam: float = ...,
            elasticNetParam: float = ...,
            tol: float = ...,
            fitIntercept: bool = ...,
            thresholds: Optional[List[float]] = ...,
            probabilityCol: str = ...,
            rawPredictionCol: str = ...,
            standardization: bool = ...,
            weightCol: Optional[str] = ...,
            aggregationDepth: int = ...,
            family: str = ...,
            lowerBoundsOnCoefficients: Optional[Matrix] = ...,
            upperBoundsOnCoefficients: Optional[Matrix] = ...,
            lowerBoundsOnIntercepts: Optional[Vector] = ...,
            upperBoundsOnIntercepts: Optional[Vector] = ...,
            maxBlockSizeInMB: float = ...,
    ):
        ...

    @keyword_only
    def __init__(
            self,
            *,
            featuresCol: str = "features",
            labelCol: str = "label",
            predictionCol: str = "prediction",
            maxIter: int = 100,
            regParam: float = 0.0,
            elasticNetParam: float = 0.0,
            tol: float = 1e-6,
            fitIntercept: bool = True,
            threshold: float = 0.5,
            thresholds: Optional[List[float]] = None,
            probabilityCol: str = "probability",
            rawPredictionCol: str = "rawPrediction",
            standardization: bool = True,
            weightCol: Optional[str] = None,
            aggregationDepth: int = 2,
            family: str = "auto",
            lowerBoundsOnCoefficients: Optional[Matrix] = None,
            upperBoundsOnCoefficients: Optional[Matrix] = None,
            lowerBoundsOnIntercepts: Optional[Vector] = None,
            upperBoundsOnIntercepts: Optional[Vector] = None,
            maxBlockSizeInMB: float = 0.0,
    ):
        """
        __init__(self, \\*, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                 threshold=0.5, thresholds=None, probabilityCol="probability", \
                 rawPredictionCol="rawPrediction", standardization=True, weightCol=None, \
                 aggregationDepth=2, family="auto", \
                 lowerBoundsOnCoefficients=None, upperBoundsOnCoefficients=None, \
                 lowerBoundsOnIntercepts=None, upperBoundsOnIntercepts=None, \
                 maxBlockSizeInMB=0.0):
        If the threshold and thresholds Params are both set, they must be equivalent.
        """
        super(LogisticRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.LogisticRegression", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._checkThresholdConsistency()

    def _algo_name(self): str = "LogisticRegression"

    def _create_model(self):
        return LogisticRegressionModel()


class LogisticRegressionModel(
    ProbabilisticClassificationModel,
    _LogisticRegressionParams,
    HasTrainingSummary["LogisticRegressionTrainingSummary"],
):
    """
    Model fitted by LogisticRegression.

    .. versionadded:: 1.3.0
    """

    @property  # type: ignore[misc]
    def coefficients(self) -> Vector:
        """
        Model coefficients of binomial logistic regression.
        An exception is thrown in the case of multinomial logistic regression.
        """
        return self._call_java("coefficients")

    @property  # type: ignore[misc]
    def intercept(self) -> float:
        """
        Model intercept of binomial logistic regression.
        An exception is thrown in the case of multinomial logistic regression.
        """
        return self._call_java("intercept")

    @property  # type: ignore[misc]
    def coefficientMatrix(self) -> Matrix:
        """
        Model coefficients.
        """
        return self._call_java("coefficientMatrix")

    @property  # type: ignore[misc]
    def interceptVector(self) -> Vector:
        """
        Model intercept.
        """
        return self._call_java("interceptVector")

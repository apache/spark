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

import os
import sys
import itertools
from multiprocessing.pool import ThreadPool

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    overload,
    TYPE_CHECKING,
)

import numpy as np

from pyspark import keyword_only, since, SparkContext, inheritable_thread_target
from pyspark.ml import Estimator, Transformer, Model
from pyspark.ml.common import inherit_doc, _py2java, _java2py
from pyspark.ml.evaluation import Evaluator, JavaEvaluator
from pyspark.ml.param import Params, Param, TypeConverters
from pyspark.ml.param.shared import HasCollectSubModels, HasParallelism, HasSeed
from pyspark.ml.util import (
    DefaultParamsReader,
    DefaultParamsWriter,
    MetaAlgorithmReadWrite,
    MLReadable,
    MLReader,
    MLWritable,
    MLWriter,
    JavaMLReader,
    JavaMLWriter,
)
from pyspark.ml.wrapper import JavaParams, JavaEstimator, JavaWrapper
from pyspark.sql.functions import col, lit, rand, UserDefinedFunction
from pyspark.sql.types import BooleanType

from pyspark.sql.dataframe import DataFrame

if TYPE_CHECKING:
    from pyspark.ml._typing import ParamMap
    from py4j.java_gateway import JavaObject
    from py4j.java_collections import JavaArray

__all__ = [
    "ParamGridBuilder",
    "CrossValidator",
    "CrossValidatorModel",
    "TrainValidationSplit",
    "TrainValidationSplitModel",
]


def _parallelFitTasks(
    est: Estimator,
    train: DataFrame,
    eva: Evaluator,
    validation: DataFrame,
    epm: Sequence["ParamMap"],
    collectSubModel: bool,
) -> List[Callable[[], Tuple[int, float, Transformer]]]:
    """
    Creates a list of callables which can be called from different threads to fit and evaluate
    an estimator in parallel. Each callable returns an `(index, metric)` pair.

    Parameters
    ----------
    est : :py:class:`pyspark.ml.baseEstimator`
        he estimator to be fit.
    train : :py:class:`pyspark.sql.DataFrame`
        DataFrame, training data set, used for fitting.
    eva : :py:class:`pyspark.ml.evaluation.Evaluator`
        used to compute `metric`
    validation : :py:class:`pyspark.sql.DataFrame`
        DataFrame, validation data set, used for evaluation.
    epm : :py:class:`collections.abc.Sequence`
        Sequence of ParamMap, params maps to be used during fitting & evaluation.
    collectSubModel : bool
        Whether to collect sub model.

    Returns
    -------
    tuple
        (int, float, subModel), an index into `epm` and the associated metric value.
    """
    modelIter = est.fitMultiple(train, epm)

    def singleTask() -> Tuple[int, float, Transformer]:
        index, model = next(modelIter)
        # TODO: duplicate evaluator to take extra params from input
        #  Note: Supporting tuning params in evaluator need update method
        #  `MetaAlgorithmReadWrite.getAllNestedStages`, make it return
        #  all nested stages and evaluators
        metric = eva.evaluate(model.transform(validation, epm[index]))
        return index, metric, model if collectSubModel else None

    return [singleTask] * len(epm)


class ParamGridBuilder:
    r"""
    Builder for a param grid used in grid search-based model selection.


    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.ml.classification import LogisticRegression
    >>> lr = LogisticRegression()
    >>> output = ParamGridBuilder() \
    ...     .baseOn({lr.labelCol: 'l'}) \
    ...     .baseOn([lr.predictionCol, 'p']) \
    ...     .addGrid(lr.regParam, [1.0, 2.0]) \
    ...     .addGrid(lr.maxIter, [1, 5]) \
    ...     .build()
    >>> expected = [
    ...     {lr.regParam: 1.0, lr.maxIter: 1, lr.labelCol: 'l', lr.predictionCol: 'p'},
    ...     {lr.regParam: 2.0, lr.maxIter: 1, lr.labelCol: 'l', lr.predictionCol: 'p'},
    ...     {lr.regParam: 1.0, lr.maxIter: 5, lr.labelCol: 'l', lr.predictionCol: 'p'},
    ...     {lr.regParam: 2.0, lr.maxIter: 5, lr.labelCol: 'l', lr.predictionCol: 'p'}]
    >>> len(output) == len(expected)
    True
    >>> all([m in expected for m in output])
    True
    """

    def __init__(self) -> None:
        self._param_grid: "ParamMap" = {}

    @since("1.4.0")
    def addGrid(self, param: Param[Any], values: List[Any]) -> "ParamGridBuilder":
        """
        Sets the given parameters in this grid to fixed values.

        param must be an instance of Param associated with an instance of Params
        (such as Estimator or Transformer).
        """
        if isinstance(param, Param):
            self._param_grid[param] = values
        else:
            raise TypeError("param must be an instance of Param")

        return self

    @overload
    def baseOn(self, __args: "ParamMap") -> "ParamGridBuilder":
        ...

    @overload
    def baseOn(self, *args: Tuple[Param, Any]) -> "ParamGridBuilder":
        ...

    @since("1.4.0")
    def baseOn(self, *args: Union["ParamMap", Tuple[Param, Any]]) -> "ParamGridBuilder":
        """
        Sets the given parameters in this grid to fixed values.
        Accepts either a parameter dictionary or a list of (parameter, value) pairs.
        """
        if isinstance(args[0], dict):
            self.baseOn(*args[0].items())
        else:
            for (param, value) in args:
                self.addGrid(param, [value])

        return self

    @since("1.4.0")
    def build(self) -> List["ParamMap"]:
        """
        Builds and returns all combinations of parameters specified
        by the param grid.
        """
        keys = self._param_grid.keys()
        grid_values = self._param_grid.values()

        def to_key_value_pairs(
            keys: Iterable[Param], values: Iterable[Any]
        ) -> Sequence[Tuple[Param, Any]]:
            return [(key, key.typeConverter(value)) for key, value in zip(keys, values)]

        return [dict(to_key_value_pairs(keys, prod)) for prod in itertools.product(*grid_values)]


class _ValidatorParams(HasSeed):
    """
    Common params for TrainValidationSplit and CrossValidator.
    """

    estimator: Param[Estimator] = Param(
        Params._dummy(), "estimator", "estimator to be cross-validated"
    )
    estimatorParamMaps: Param[List["ParamMap"]] = Param(
        Params._dummy(), "estimatorParamMaps", "estimator param maps"
    )
    evaluator: Param[Evaluator] = Param(
        Params._dummy(),
        "evaluator",
        "evaluator used to select hyper-parameters that maximize the validator metric",
    )

    @since("2.0.0")
    def getEstimator(self) -> Estimator:
        """
        Gets the value of estimator or its default value.
        """
        return self.getOrDefault(self.estimator)

    @since("2.0.0")
    def getEstimatorParamMaps(self) -> List["ParamMap"]:
        """
        Gets the value of estimatorParamMaps or its default value.
        """
        return self.getOrDefault(self.estimatorParamMaps)

    @since("2.0.0")
    def getEvaluator(self) -> Evaluator:
        """
        Gets the value of evaluator or its default value.
        """
        return self.getOrDefault(self.evaluator)

    @classmethod
    def _from_java_impl(
        cls, java_stage: "JavaObject"
    ) -> Tuple[Estimator, List["ParamMap"], Evaluator]:
        """
        Return Python estimator, estimatorParamMaps, and evaluator from a Java ValidatorParams.
        """

        # Load information from java_stage to the instance.
        estimator: Estimator = JavaParams._from_java(java_stage.getEstimator())
        evaluator: Evaluator = JavaParams._from_java(java_stage.getEvaluator())
        if isinstance(estimator, JavaEstimator):
            epms = [
                estimator._transfer_param_map_from_java(epm)
                for epm in java_stage.getEstimatorParamMaps()
            ]
        elif MetaAlgorithmReadWrite.isMetaEstimator(estimator):
            # Meta estimator such as Pipeline, OneVsRest
            epms = _ValidatorSharedReadWrite.meta_estimator_transfer_param_maps_from_java(
                estimator, java_stage.getEstimatorParamMaps()
            )
        else:
            raise ValueError("Unsupported estimator used in tuning: " + str(estimator))

        return estimator, epms, evaluator

    def _to_java_impl(self) -> Tuple["JavaObject", "JavaObject", "JavaObject"]:
        """
        Return Java estimator, estimatorParamMaps, and evaluator from this Python instance.
        """

        gateway = SparkContext._gateway
        assert gateway is not None and SparkContext._jvm is not None

        cls = SparkContext._jvm.org.apache.spark.ml.param.ParamMap

        estimator = self.getEstimator()
        if isinstance(estimator, JavaEstimator):
            java_epms = gateway.new_array(cls, len(self.getEstimatorParamMaps()))
            for idx, epm in enumerate(self.getEstimatorParamMaps()):
                java_epms[idx] = estimator._transfer_param_map_to_java(epm)
        elif MetaAlgorithmReadWrite.isMetaEstimator(estimator):
            # Meta estimator such as Pipeline, OneVsRest
            java_epms = _ValidatorSharedReadWrite.meta_estimator_transfer_param_maps_to_java(
                estimator, self.getEstimatorParamMaps()
            )
        else:
            raise ValueError("Unsupported estimator used in tuning: " + str(estimator))

        java_estimator = cast(JavaEstimator, self.getEstimator())._to_java()
        java_evaluator = cast(JavaEvaluator, self.getEvaluator())._to_java()
        return java_estimator, java_epms, java_evaluator


class _ValidatorSharedReadWrite:
    @staticmethod
    def meta_estimator_transfer_param_maps_to_java(
        pyEstimator: Estimator, pyParamMaps: Sequence["ParamMap"]
    ) -> "JavaArray":
        pyStages = MetaAlgorithmReadWrite.getAllNestedStages(pyEstimator)
        stagePairs = list(map(lambda stage: (stage, cast(JavaParams, stage)._to_java()), pyStages))
        sc = SparkContext._active_spark_context

        assert (
            sc is not None and SparkContext._jvm is not None and SparkContext._gateway is not None
        )

        paramMapCls = SparkContext._jvm.org.apache.spark.ml.param.ParamMap
        javaParamMaps = SparkContext._gateway.new_array(paramMapCls, len(pyParamMaps))

        for idx, pyParamMap in enumerate(pyParamMaps):
            javaParamMap = JavaWrapper._new_java_obj("org.apache.spark.ml.param.ParamMap")
            for pyParam, pyValue in pyParamMap.items():
                javaParam = None
                for pyStage, javaStage in stagePairs:
                    if pyStage._testOwnParam(pyParam.parent, pyParam.name):
                        javaParam = javaStage.getParam(pyParam.name)
                        break
                if javaParam is None:
                    raise ValueError("Resolve param in estimatorParamMaps failed: " + str(pyParam))
                if isinstance(pyValue, Params) and hasattr(pyValue, "_to_java"):
                    javaValue = cast(JavaParams, pyValue)._to_java()
                else:
                    javaValue = _py2java(sc, pyValue)
                pair = javaParam.w(javaValue)
                javaParamMap.put([pair])
            javaParamMaps[idx] = javaParamMap
        return javaParamMaps

    @staticmethod
    def meta_estimator_transfer_param_maps_from_java(
        pyEstimator: Estimator, javaParamMaps: "JavaArray"
    ) -> List["ParamMap"]:
        pyStages = MetaAlgorithmReadWrite.getAllNestedStages(pyEstimator)
        stagePairs = list(map(lambda stage: (stage, cast(JavaParams, stage)._to_java()), pyStages))
        sc = SparkContext._active_spark_context

        assert sc is not None and sc._jvm is not None

        pyParamMaps = []
        for javaParamMap in javaParamMaps:
            pyParamMap = dict()
            for javaPair in javaParamMap.toList():
                javaParam = javaPair.param()
                pyParam = None
                for pyStage, javaStage in stagePairs:
                    if pyStage._testOwnParam(javaParam.parent(), javaParam.name()):
                        pyParam = pyStage.getParam(javaParam.name())
                if pyParam is None:
                    raise ValueError(
                        "Resolve param in estimatorParamMaps failed: "
                        + javaParam.parent()
                        + "."
                        + javaParam.name()
                    )
                javaValue = javaPair.value()
                pyValue: Any
                if sc._jvm.Class.forName(
                    "org.apache.spark.ml.util.DefaultParamsWritable"
                ).isInstance(javaValue):
                    pyValue = JavaParams._from_java(javaValue)
                else:
                    pyValue = _java2py(sc, javaValue)
                pyParamMap[pyParam] = pyValue
            pyParamMaps.append(pyParamMap)
        return pyParamMaps

    @staticmethod
    def is_java_convertible(instance: _ValidatorParams) -> bool:
        allNestedStages = MetaAlgorithmReadWrite.getAllNestedStages(instance.getEstimator())
        evaluator_convertible = isinstance(instance.getEvaluator(), JavaParams)
        estimator_convertible = all(map(lambda stage: hasattr(stage, "_to_java"), allNestedStages))
        return estimator_convertible and evaluator_convertible

    @staticmethod
    def saveImpl(
        path: str,
        instance: _ValidatorParams,
        sc: SparkContext,
        extraMetadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        numParamsNotJson = 0
        jsonEstimatorParamMaps = []
        for paramMap in instance.getEstimatorParamMaps():
            jsonParamMap = []
            for p, v in paramMap.items():
                jsonParam: Dict[str, Any] = {"parent": p.parent, "name": p.name}
                if (
                    (isinstance(v, Estimator) and not MetaAlgorithmReadWrite.isMetaEstimator(v))
                    or isinstance(v, Transformer)
                    or isinstance(v, Evaluator)
                ):
                    relative_path = f"epm_{p.name}{numParamsNotJson}"
                    param_path = os.path.join(path, relative_path)
                    numParamsNotJson += 1
                    cast(MLWritable, v).save(param_path)
                    jsonParam["value"] = relative_path
                    jsonParam["isJson"] = False
                elif isinstance(v, MLWritable):
                    raise RuntimeError(
                        "ValidatorSharedReadWrite.saveImpl does not handle parameters of type: "
                        "MLWritable that are not Estimator/Evaluator/Transformer, and if parameter "
                        "is estimator, it cannot be meta estimator such as Validator or OneVsRest"
                    )
                else:
                    jsonParam["value"] = v
                    jsonParam["isJson"] = True
                jsonParamMap.append(jsonParam)
            jsonEstimatorParamMaps.append(jsonParamMap)

        skipParams = ["estimator", "evaluator", "estimatorParamMaps"]
        jsonParams = DefaultParamsWriter.extractJsonParams(instance, skipParams)
        jsonParams["estimatorParamMaps"] = jsonEstimatorParamMaps

        DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, jsonParams)
        evaluatorPath = os.path.join(path, "evaluator")
        cast(MLWritable, instance.getEvaluator()).save(evaluatorPath)
        estimatorPath = os.path.join(path, "estimator")
        cast(MLWritable, instance.getEstimator()).save(estimatorPath)

    @staticmethod
    def load(
        path: str, sc: SparkContext, metadata: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Estimator, Evaluator, List["ParamMap"]]:
        evaluatorPath = os.path.join(path, "evaluator")
        evaluator: Evaluator = DefaultParamsReader.loadParamsInstance(evaluatorPath, sc)
        estimatorPath = os.path.join(path, "estimator")
        estimator: Estimator = DefaultParamsReader.loadParamsInstance(estimatorPath, sc)

        uidToParams = MetaAlgorithmReadWrite.getUidMap(estimator)
        uidToParams[evaluator.uid] = evaluator

        jsonEstimatorParamMaps = metadata["paramMap"]["estimatorParamMaps"]

        estimatorParamMaps = []
        for jsonParamMap in jsonEstimatorParamMaps:
            paramMap = {}
            for jsonParam in jsonParamMap:
                est = uidToParams[jsonParam["parent"]]
                param = getattr(est, jsonParam["name"])
                if "isJson" not in jsonParam or ("isJson" in jsonParam and jsonParam["isJson"]):
                    value = jsonParam["value"]
                else:
                    relativePath = jsonParam["value"]
                    valueSavedPath = os.path.join(path, relativePath)
                    value = DefaultParamsReader.loadParamsInstance(valueSavedPath, sc)
                paramMap[param] = value
            estimatorParamMaps.append(paramMap)

        return metadata, estimator, evaluator, estimatorParamMaps

    @staticmethod
    def validateParams(instance: _ValidatorParams) -> None:
        estiamtor = instance.getEstimator()
        evaluator = instance.getEvaluator()
        uidMap = MetaAlgorithmReadWrite.getUidMap(estiamtor)

        for elem in [evaluator] + list(uidMap.values()):
            if not isinstance(elem, MLWritable):
                raise ValueError(
                    f"Validator write will fail because it contains {elem.uid} "
                    f"which is not writable."
                )

        estimatorParamMaps = instance.getEstimatorParamMaps()
        paramErr = (
            "Validator save requires all Params in estimatorParamMaps to apply to "
            "its Estimator, An extraneous Param was found: "
        )
        for paramMap in estimatorParamMaps:
            for param in paramMap:
                if param.parent not in uidMap:
                    raise ValueError(paramErr + repr(param))

    @staticmethod
    def getValidatorModelWriterPersistSubModelsParam(writer: MLWriter) -> bool:
        if "persistsubmodels" in writer.optionMap:
            persistSubModelsParam = writer.optionMap["persistsubmodels"].lower()
            if persistSubModelsParam == "true":
                return True
            elif persistSubModelsParam == "false":
                return False
            else:
                raise ValueError(
                    f"persistSubModels option value {persistSubModelsParam} is invalid, "
                    f"the possible values are True, 'True' or False, 'False'"
                )
        else:
            return writer.instance.subModels is not None  # type: ignore[attr-defined]


_save_with_persist_submodels_no_submodels_found_err: str = (
    "When persisting tuning models, you can only set persistSubModels to true if the tuning "
    "was done with collectSubModels set to true. To save the sub-models, try rerunning fitting "
    "with collectSubModels set to true."
)


@inherit_doc
class CrossValidatorReader(MLReader["CrossValidator"]):
    def __init__(self, cls: Type["CrossValidator"]):
        super(CrossValidatorReader, self).__init__()
        self.cls = cls

    def load(self, path: str) -> "CrossValidator":
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        if not DefaultParamsReader.isPythonParamsInstance(metadata):
            return JavaMLReader(self.cls).load(path)  # type: ignore[arg-type]
        else:
            metadata, estimator, evaluator, estimatorParamMaps = _ValidatorSharedReadWrite.load(
                path, self.sc, metadata
            )
            cv = CrossValidator(
                estimator=estimator, estimatorParamMaps=estimatorParamMaps, evaluator=evaluator
            )
            cv = cv._resetUid(metadata["uid"])
            DefaultParamsReader.getAndSetParams(cv, metadata, skipParams=["estimatorParamMaps"])
            return cv


@inherit_doc
class CrossValidatorWriter(MLWriter):
    def __init__(self, instance: "CrossValidator"):
        super(CrossValidatorWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        _ValidatorSharedReadWrite.validateParams(self.instance)
        _ValidatorSharedReadWrite.saveImpl(path, self.instance, self.sc)


@inherit_doc
class CrossValidatorModelReader(MLReader["CrossValidatorModel"]):
    def __init__(self, cls: Type["CrossValidatorModel"]):
        super(CrossValidatorModelReader, self).__init__()
        self.cls = cls

    def load(self, path: str) -> "CrossValidatorModel":
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        if not DefaultParamsReader.isPythonParamsInstance(metadata):
            return JavaMLReader(self.cls).load(path)  # type: ignore[arg-type]
        else:
            metadata, estimator, evaluator, estimatorParamMaps = _ValidatorSharedReadWrite.load(
                path, self.sc, metadata
            )
            numFolds = metadata["paramMap"]["numFolds"]
            bestModelPath = os.path.join(path, "bestModel")
            bestModel: Model = DefaultParamsReader.loadParamsInstance(bestModelPath, self.sc)
            avgMetrics = metadata["avgMetrics"]
            if "stdMetrics" in metadata:
                stdMetrics = metadata["stdMetrics"]
            else:
                stdMetrics = None
            persistSubModels = ("persistSubModels" in metadata) and metadata["persistSubModels"]

            if persistSubModels:
                subModels = [[None] * len(estimatorParamMaps)] * numFolds
                for splitIndex in range(numFolds):
                    for paramIndex in range(len(estimatorParamMaps)):
                        modelPath = os.path.join(
                            path, "subModels", f"fold{splitIndex}", f"{paramIndex}"
                        )
                        subModels[splitIndex][paramIndex] = DefaultParamsReader.loadParamsInstance(
                            modelPath, self.sc
                        )
            else:
                subModels = None

            cvModel = CrossValidatorModel(
                bestModel,
                avgMetrics=avgMetrics,
                subModels=cast(List[List[Model]], subModels),
                stdMetrics=stdMetrics,
            )
            cvModel = cvModel._resetUid(metadata["uid"])
            cvModel.set(cvModel.estimator, estimator)
            cvModel.set(cvModel.estimatorParamMaps, estimatorParamMaps)
            cvModel.set(cvModel.evaluator, evaluator)
            DefaultParamsReader.getAndSetParams(
                cvModel, metadata, skipParams=["estimatorParamMaps"]
            )
            return cvModel


@inherit_doc
class CrossValidatorModelWriter(MLWriter):
    def __init__(self, instance: "CrossValidatorModel"):
        super(CrossValidatorModelWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        _ValidatorSharedReadWrite.validateParams(self.instance)
        instance = self.instance
        persistSubModels = _ValidatorSharedReadWrite.getValidatorModelWriterPersistSubModelsParam(
            self
        )
        extraMetadata = {"avgMetrics": instance.avgMetrics, "persistSubModels": persistSubModels}
        if instance.stdMetrics:
            extraMetadata["stdMetrics"] = instance.stdMetrics

        _ValidatorSharedReadWrite.saveImpl(path, instance, self.sc, extraMetadata=extraMetadata)
        bestModelPath = os.path.join(path, "bestModel")
        cast(MLWritable, instance.bestModel).save(bestModelPath)
        if persistSubModels:
            if instance.subModels is None:
                raise ValueError(_save_with_persist_submodels_no_submodels_found_err)
            subModelsPath = os.path.join(path, "subModels")
            for splitIndex in range(instance.getNumFolds()):
                splitPath = os.path.join(subModelsPath, f"fold{splitIndex}")
                for paramIndex in range(len(instance.getEstimatorParamMaps())):
                    modelPath = os.path.join(splitPath, f"{paramIndex}")
                    cast(MLWritable, instance.subModels[splitIndex][paramIndex]).save(modelPath)


class _CrossValidatorParams(_ValidatorParams):
    """
    Params for :py:class:`CrossValidator` and :py:class:`CrossValidatorModel`.

    .. versionadded:: 3.0.0
    """

    numFolds: Param[int] = Param(
        Params._dummy(),
        "numFolds",
        "number of folds for cross validation",
        typeConverter=TypeConverters.toInt,
    )

    foldCol: Param[str] = Param(
        Params._dummy(),
        "foldCol",
        "Param for the column name of user "
        + "specified fold number. Once this is specified, :py:class:`CrossValidator` "
        + "won't do random k-fold split. Note that this column should be integer type "
        + "with range [0, numFolds) and Spark will throw exception on out-of-range "
        + "fold numbers.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_CrossValidatorParams, self).__init__(*args)
        self._setDefault(numFolds=3, foldCol="")

    @since("1.4.0")
    def getNumFolds(self) -> int:
        """
        Gets the value of numFolds or its default value.
        """
        return self.getOrDefault(self.numFolds)

    @since("3.1.0")
    def getFoldCol(self) -> str:
        """
        Gets the value of foldCol or its default value.
        """
        return self.getOrDefault(self.foldCol)


class CrossValidator(
    Estimator["CrossValidatorModel"],
    _CrossValidatorParams,
    HasParallelism,
    HasCollectSubModels,
    MLReadable["CrossValidator"],
    MLWritable,
):
    """

    K-fold cross validation performs model selection by splitting the dataset into a set of
    non-overlapping randomly partitioned folds which are used as separate training and test datasets
    e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs,
    each of which uses 2/3 of the data for training and 1/3 for testing. Each fold is used as the
    test set exactly once.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.ml.classification import LogisticRegression
    >>> from pyspark.ml.evaluation import BinaryClassificationEvaluator
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
    >>> import tempfile
    >>> dataset = spark.createDataFrame(
    ...     [(Vectors.dense([0.0]), 0.0),
    ...      (Vectors.dense([0.4]), 1.0),
    ...      (Vectors.dense([0.5]), 0.0),
    ...      (Vectors.dense([0.6]), 1.0),
    ...      (Vectors.dense([1.0]), 1.0)] * 10,
    ...     ["features", "label"])
    >>> lr = LogisticRegression()
    >>> grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
    >>> evaluator = BinaryClassificationEvaluator()
    >>> cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator,
    ...     parallelism=2)
    >>> cvModel = cv.fit(dataset)
    >>> cvModel.getNumFolds()
    3
    >>> cvModel.avgMetrics[0]
    0.5
    >>> path = tempfile.mkdtemp()
    >>> model_path = path + "/model"
    >>> cvModel.write().save(model_path)
    >>> cvModelRead = CrossValidatorModel.read().load(model_path)
    >>> cvModelRead.avgMetrics
    [0.5, ...
    >>> evaluator.evaluate(cvModel.transform(dataset))
    0.8333...
    >>> evaluator.evaluate(cvModelRead.transform(dataset))
    0.8333...
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        estimator: Optional[Estimator] = None,
        estimatorParamMaps: Optional[List["ParamMap"]] = None,
        evaluator: Optional[Evaluator] = None,
        numFolds: int = 3,
        seed: Optional[int] = None,
        parallelism: int = 1,
        collectSubModels: bool = False,
        foldCol: str = "",
    ) -> None:
        """
        __init__(self, \\*, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                 seed=None, parallelism=1, collectSubModels=False, foldCol="")
        """
        super(CrossValidator, self).__init__()
        self._setDefault(parallelism=1)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(
        self,
        *,
        estimator: Optional[Estimator] = None,
        estimatorParamMaps: Optional[List["ParamMap"]] = None,
        evaluator: Optional[Evaluator] = None,
        numFolds: int = 3,
        seed: Optional[int] = None,
        parallelism: int = 1,
        collectSubModels: bool = False,
        foldCol: str = "",
    ) -> "CrossValidator":
        """
        setParams(self, \\*, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                  seed=None, parallelism=1, collectSubModels=False, foldCol=""):
        Sets params for cross validator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setEstimator(self, value: Estimator) -> "CrossValidator":
        """
        Sets the value of :py:attr:`estimator`.
        """
        return self._set(estimator=value)

    @since("2.0.0")
    def setEstimatorParamMaps(self, value: List["ParamMap"]) -> "CrossValidator":
        """
        Sets the value of :py:attr:`estimatorParamMaps`.
        """
        return self._set(estimatorParamMaps=value)

    @since("2.0.0")
    def setEvaluator(self, value: Evaluator) -> "CrossValidator":
        """
        Sets the value of :py:attr:`evaluator`.
        """
        return self._set(evaluator=value)

    @since("1.4.0")
    def setNumFolds(self, value: int) -> "CrossValidator":
        """
        Sets the value of :py:attr:`numFolds`.
        """
        return self._set(numFolds=value)

    @since("3.1.0")
    def setFoldCol(self, value: str) -> "CrossValidator":
        """
        Sets the value of :py:attr:`foldCol`.
        """
        return self._set(foldCol=value)

    def setSeed(self, value: int) -> "CrossValidator":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def setParallelism(self, value: int) -> "CrossValidator":
        """
        Sets the value of :py:attr:`parallelism`.
        """
        return self._set(parallelism=value)

    def setCollectSubModels(self, value: bool) -> "CrossValidator":
        """
        Sets the value of :py:attr:`collectSubModels`.
        """
        return self._set(collectSubModels=value)

    @staticmethod
    def _gen_avg_and_std_metrics(metrics_all: List[List[float]]) -> Tuple[List[float], List[float]]:
        avg_metrics = np.mean(metrics_all, axis=0)
        std_metrics = np.std(metrics_all, axis=0)
        return list(avg_metrics), list(std_metrics)

    def _fit(self, dataset: DataFrame) -> "CrossValidatorModel":
        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        eva = self.getOrDefault(self.evaluator)
        nFolds = self.getOrDefault(self.numFolds)
        metrics_all = [[0.0] * numModels for i in range(nFolds)]

        pool = ThreadPool(processes=min(self.getParallelism(), numModels))
        subModels = None
        collectSubModelsParam = self.getCollectSubModels()
        if collectSubModelsParam:
            subModels = [[None for j in range(numModels)] for i in range(nFolds)]

        datasets = self._kFold(dataset)
        for i in range(nFolds):
            validation = datasets[i][1].cache()
            train = datasets[i][0].cache()

            tasks = map(
                inheritable_thread_target,
                _parallelFitTasks(est, train, eva, validation, epm, collectSubModelsParam),
            )
            for j, metric, subModel in pool.imap_unordered(lambda f: f(), tasks):
                metrics_all[i][j] = metric
                if collectSubModelsParam:
                    assert subModels is not None
                    subModels[i][j] = subModel

            validation.unpersist()
            train.unpersist()

        metrics, std_metrics = CrossValidator._gen_avg_and_std_metrics(metrics_all)

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)
        bestModel = est.fit(dataset, epm[bestIndex])
        return self._copyValues(
            CrossValidatorModel(bestModel, metrics, cast(List[List[Model]], subModels), std_metrics)
        )

    def _kFold(self, dataset: DataFrame) -> List[Tuple[DataFrame, DataFrame]]:
        nFolds = self.getOrDefault(self.numFolds)
        foldCol = self.getOrDefault(self.foldCol)

        datasets = []
        if not foldCol:
            # Do random k-fold split.
            seed = self.getOrDefault(self.seed)
            h = 1.0 / nFolds
            randCol = self.uid + "_rand"
            df = dataset.select("*", rand(seed).alias(randCol))
            for i in range(nFolds):
                validateLB = i * h
                validateUB = (i + 1) * h
                condition = (df[randCol] >= validateLB) & (df[randCol] < validateUB)
                validation = df.filter(condition)
                train = df.filter(~condition)
                datasets.append((train, validation))
        else:
            # Use user-specified fold numbers.
            def checker(foldNum: int) -> bool:
                if foldNum < 0 or foldNum >= nFolds:
                    raise ValueError(
                        "Fold number must be in range [0, %s), but got %s." % (nFolds, foldNum)
                    )
                return True

            checker_udf = UserDefinedFunction(checker, BooleanType())
            for i in range(nFolds):
                training = dataset.filter(checker_udf(dataset[foldCol]) & (col(foldCol) != lit(i)))
                validation = dataset.filter(
                    checker_udf(dataset[foldCol]) & (col(foldCol) == lit(i))
                )
                if training.rdd.getNumPartitions() == 0 or len(training.take(1)) == 0:
                    raise ValueError("The training data at fold %s is empty." % i)
                if validation.rdd.getNumPartitions() == 0 or len(validation.take(1)) == 0:
                    raise ValueError("The validation data at fold %s is empty." % i)
                datasets.append((training, validation))

        return datasets

    def copy(self, extra: Optional["ParamMap"] = None) -> "CrossValidator":
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies creates a deep copy of
        the embedded paramMap, and copies the embedded and extra parameters over.


        .. versionadded:: 1.4.0

        Parameters
        ----------
        extra : dict, optional
            Extra parameters to copy to the new instance

        Returns
        -------
        :py:class:`CrossValidator`
            Copy of this instance
        """
        if extra is None:
            extra = dict()
        newCV = Params.copy(self, extra)
        if self.isSet(self.estimator):
            newCV.setEstimator(self.getEstimator().copy(extra))
        # estimatorParamMaps remain the same
        if self.isSet(self.evaluator):
            newCV.setEvaluator(self.getEvaluator().copy(extra))
        return newCV

    @since("2.3.0")
    def write(self) -> MLWriter:
        """Returns an MLWriter instance for this ML instance."""
        if _ValidatorSharedReadWrite.is_java_convertible(self):
            return JavaMLWriter(self)  # type: ignore[arg-type]
        return CrossValidatorWriter(self)

    @classmethod
    @since("2.3.0")
    def read(cls) -> CrossValidatorReader:
        """Returns an MLReader instance for this class."""
        return CrossValidatorReader(cls)

    @classmethod
    def _from_java(cls, java_stage: "JavaObject") -> "CrossValidator":
        """
        Given a Java CrossValidator, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        estimator, epms, evaluator = super(CrossValidator, cls)._from_java_impl(java_stage)
        numFolds = java_stage.getNumFolds()
        seed = java_stage.getSeed()
        parallelism = java_stage.getParallelism()
        collectSubModels = java_stage.getCollectSubModels()
        foldCol = java_stage.getFoldCol()
        # Create a new instance of this stage.
        py_stage = cls(
            estimator=estimator,
            estimatorParamMaps=epms,
            evaluator=evaluator,
            numFolds=numFolds,
            seed=seed,
            parallelism=parallelism,
            collectSubModels=collectSubModels,
            foldCol=foldCol,
        )
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self) -> "JavaObject":
        """
        Transfer this instance to a Java CrossValidator. Used for ML persistence.

        Returns
        -------
        py4j.java_gateway.JavaObject
            Java object equivalent to this instance.
        """

        estimator, epms, evaluator = super(CrossValidator, self)._to_java_impl()

        _java_obj = JavaParams._new_java_obj("org.apache.spark.ml.tuning.CrossValidator", self.uid)
        _java_obj.setEstimatorParamMaps(epms)
        _java_obj.setEvaluator(evaluator)
        _java_obj.setEstimator(estimator)
        _java_obj.setSeed(self.getSeed())
        _java_obj.setNumFolds(self.getNumFolds())
        _java_obj.setParallelism(self.getParallelism())
        _java_obj.setCollectSubModels(self.getCollectSubModels())
        _java_obj.setFoldCol(self.getFoldCol())

        return _java_obj


class CrossValidatorModel(
    Model, _CrossValidatorParams, MLReadable["CrossValidatorModel"], MLWritable
):
    """
    CrossValidatorModel contains the model with the highest average cross-validation
    metric across folds and uses this model to transform input data. CrossValidatorModel
    also tracks the metrics for each param map evaluated.

    .. versionadded:: 1.4.0

    Notes
    -----
    Since version 3.3.0, CrossValidatorModel contains a new attribute "stdMetrics",
    which represent standard deviation of metrics for each paramMap in
    CrossValidator.estimatorParamMaps.
    """

    def __init__(
        self,
        bestModel: Model,
        avgMetrics: Optional[List[float]] = None,
        subModels: Optional[List[List[Model]]] = None,
        stdMetrics: Optional[List[float]] = None,
    ):
        super(CrossValidatorModel, self).__init__()
        #: best model from cross validation
        self.bestModel = bestModel
        #: Average cross-validation metrics for each paramMap in
        #: CrossValidator.estimatorParamMaps, in the corresponding order.
        self.avgMetrics = avgMetrics or []
        #: sub model list from cross validation
        self.subModels = subModels
        #: standard deviation of metrics for each paramMap in
        #: CrossValidator.estimatorParamMaps, in the corresponding order.
        self.stdMetrics = stdMetrics or []

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return self.bestModel.transform(dataset)

    def copy(self, extra: Optional["ParamMap"] = None) -> "CrossValidatorModel":
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies the underlying bestModel,
        creates a deep copy of the embedded paramMap, and
        copies the embedded and extra parameters over.
        It does not copy the extra Params into the subModels.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        extra : dict, optional
            Extra parameters to copy to the new instance

        Returns
        -------
        :py:class:`CrossValidatorModel`
            Copy of this instance
        """
        if extra is None:
            extra = dict()
        bestModel = self.bestModel.copy(extra)
        avgMetrics = list(self.avgMetrics)
        assert self.subModels is not None
        subModels = [
            [sub_model.copy() for sub_model in fold_sub_models]
            for fold_sub_models in self.subModels
        ]
        stdMetrics = list(self.stdMetrics)
        return self._copyValues(
            CrossValidatorModel(bestModel, avgMetrics, subModels, stdMetrics), extra=extra
        )

    @since("2.3.0")
    def write(self) -> MLWriter:
        """Returns an MLWriter instance for this ML instance."""
        if _ValidatorSharedReadWrite.is_java_convertible(self):
            return JavaMLWriter(self)  # type: ignore[arg-type]
        return CrossValidatorModelWriter(self)

    @classmethod
    @since("2.3.0")
    def read(cls) -> CrossValidatorModelReader:
        """Returns an MLReader instance for this class."""
        return CrossValidatorModelReader(cls)

    @classmethod
    def _from_java(cls, java_stage: "JavaObject") -> "CrossValidatorModel":
        """
        Given a Java CrossValidatorModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """
        sc = SparkContext._active_spark_context
        assert sc is not None

        bestModel: Model = JavaParams._from_java(java_stage.bestModel())
        avgMetrics = _java2py(sc, java_stage.avgMetrics())
        estimator, epms, evaluator = super(CrossValidatorModel, cls)._from_java_impl(java_stage)

        py_stage = cls(bestModel=bestModel, avgMetrics=avgMetrics)
        params = {
            "evaluator": evaluator,
            "estimator": estimator,
            "estimatorParamMaps": epms,
            "numFolds": java_stage.getNumFolds(),
            "foldCol": java_stage.getFoldCol(),
            "seed": java_stage.getSeed(),
        }
        for param_name, param_val in params.items():
            py_stage = py_stage._set(**{param_name: param_val})

        if java_stage.hasSubModels():
            py_stage.subModels = [
                [JavaParams._from_java(sub_model) for sub_model in fold_sub_models]
                for fold_sub_models in java_stage.subModels()
            ]

        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self) -> "JavaObject":
        """
        Transfer this instance to a Java CrossValidatorModel. Used for ML persistence.

        Returns
        -------
        py4j.java_gateway.JavaObject
            Java object equivalent to this instance.
        """

        sc = SparkContext._active_spark_context
        assert sc is not None

        _java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.tuning.CrossValidatorModel",
            self.uid,
            cast(JavaParams, self.bestModel)._to_java(),
            _py2java(sc, self.avgMetrics),
        )
        estimator, epms, evaluator = super(CrossValidatorModel, self)._to_java_impl()

        params = {
            "evaluator": evaluator,
            "estimator": estimator,
            "estimatorParamMaps": epms,
            "numFolds": self.getNumFolds(),
            "foldCol": self.getFoldCol(),
            "seed": self.getSeed(),
        }
        for param_name, param_val in params.items():
            java_param = _java_obj.getParam(param_name)
            pair = java_param.w(param_val)
            _java_obj.set(pair)

        if self.subModels is not None:
            java_sub_models = [
                [cast(JavaParams, sub_model)._to_java() for sub_model in fold_sub_models]
                for fold_sub_models in self.subModels
            ]
            _java_obj.setSubModels(java_sub_models)
        return _java_obj


@inherit_doc
class TrainValidationSplitReader(MLReader["TrainValidationSplit"]):
    def __init__(self, cls: Type["TrainValidationSplit"]):
        super(TrainValidationSplitReader, self).__init__()
        self.cls = cls

    def load(self, path: str) -> "TrainValidationSplit":
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        if not DefaultParamsReader.isPythonParamsInstance(metadata):
            return JavaMLReader(self.cls).load(path)  # type: ignore[arg-type]
        else:
            metadata, estimator, evaluator, estimatorParamMaps = _ValidatorSharedReadWrite.load(
                path, self.sc, metadata
            )
            tvs = TrainValidationSplit(
                estimator=estimator, estimatorParamMaps=estimatorParamMaps, evaluator=evaluator
            )
            tvs = tvs._resetUid(metadata["uid"])
            DefaultParamsReader.getAndSetParams(tvs, metadata, skipParams=["estimatorParamMaps"])
            return tvs


@inherit_doc
class TrainValidationSplitWriter(MLWriter):
    def __init__(self, instance: "TrainValidationSplit"):
        super(TrainValidationSplitWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        _ValidatorSharedReadWrite.validateParams(self.instance)
        _ValidatorSharedReadWrite.saveImpl(path, self.instance, self.sc)


@inherit_doc
class TrainValidationSplitModelReader(MLReader["TrainValidationSplitModel"]):
    def __init__(self, cls: Type["TrainValidationSplitModel"]):
        super(TrainValidationSplitModelReader, self).__init__()
        self.cls = cls

    def load(self, path: str) -> "TrainValidationSplitModel":
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        if not DefaultParamsReader.isPythonParamsInstance(metadata):
            return JavaMLReader(self.cls).load(path)  # type: ignore[arg-type]
        else:
            metadata, estimator, evaluator, estimatorParamMaps = _ValidatorSharedReadWrite.load(
                path, self.sc, metadata
            )
            bestModelPath = os.path.join(path, "bestModel")
            bestModel: Model = DefaultParamsReader.loadParamsInstance(bestModelPath, self.sc)
            validationMetrics = metadata["validationMetrics"]
            persistSubModels = ("persistSubModels" in metadata) and metadata["persistSubModels"]

            if persistSubModels:
                subModels = [None] * len(estimatorParamMaps)
                for paramIndex in range(len(estimatorParamMaps)):
                    modelPath = os.path.join(path, "subModels", f"{paramIndex}")
                    subModels[paramIndex] = DefaultParamsReader.loadParamsInstance(
                        modelPath, self.sc
                    )
            else:
                subModels = None

            tvsModel = TrainValidationSplitModel(
                bestModel,
                validationMetrics=validationMetrics,
                subModels=cast(Optional[List[Model]], subModels),
            )
            tvsModel = tvsModel._resetUid(metadata["uid"])
            tvsModel.set(tvsModel.estimator, estimator)
            tvsModel.set(tvsModel.estimatorParamMaps, estimatorParamMaps)
            tvsModel.set(tvsModel.evaluator, evaluator)
            DefaultParamsReader.getAndSetParams(
                tvsModel, metadata, skipParams=["estimatorParamMaps"]
            )
            return tvsModel


@inherit_doc
class TrainValidationSplitModelWriter(MLWriter):
    def __init__(self, instance: "TrainValidationSplitModel"):
        super(TrainValidationSplitModelWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        _ValidatorSharedReadWrite.validateParams(self.instance)
        instance = self.instance
        persistSubModels = _ValidatorSharedReadWrite.getValidatorModelWriterPersistSubModelsParam(
            self
        )

        extraMetadata = {
            "validationMetrics": instance.validationMetrics,
            "persistSubModels": persistSubModels,
        }
        _ValidatorSharedReadWrite.saveImpl(path, instance, self.sc, extraMetadata=extraMetadata)
        bestModelPath = os.path.join(path, "bestModel")
        cast(MLWritable, instance.bestModel).save(bestModelPath)
        if persistSubModels:
            if instance.subModels is None:
                raise ValueError(_save_with_persist_submodels_no_submodels_found_err)
            subModelsPath = os.path.join(path, "subModels")
            for paramIndex in range(len(instance.getEstimatorParamMaps())):
                modelPath = os.path.join(subModelsPath, f"{paramIndex}")
                cast(MLWritable, instance.subModels[paramIndex]).save(modelPath)


class _TrainValidationSplitParams(_ValidatorParams):
    """
    Params for :py:class:`TrainValidationSplit` and :py:class:`TrainValidationSplitModel`.

    .. versionadded:: 3.0.0
    """

    trainRatio: Param[float] = Param(
        Params._dummy(),
        "trainRatio",
        "Param for ratio between train and\
     validation data. Must be between 0 and 1.",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self, *args: Any):
        super(_TrainValidationSplitParams, self).__init__(*args)
        self._setDefault(trainRatio=0.75)

    @since("2.0.0")
    def getTrainRatio(self) -> float:
        """
        Gets the value of trainRatio or its default value.
        """
        return self.getOrDefault(self.trainRatio)


class TrainValidationSplit(
    Estimator["TrainValidationSplitModel"],
    _TrainValidationSplitParams,
    HasParallelism,
    HasCollectSubModels,
    MLReadable["TrainValidationSplit"],
    MLWritable,
):
    """
    Validation for hyper-parameter tuning. Randomly splits the input dataset into train and
    validation sets, and uses evaluation metric on the validation set to select the best model.
    Similar to :class:`CrossValidator`, but only splits the set once.

    .. versionadded:: 2.0.0

    Examples
    --------
    >>> from pyspark.ml.classification import LogisticRegression
    >>> from pyspark.ml.evaluation import BinaryClassificationEvaluator
    >>> from pyspark.ml.linalg import Vectors
    >>> from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
    >>> from pyspark.ml.tuning import TrainValidationSplitModel
    >>> import tempfile
    >>> dataset = spark.createDataFrame(
    ...     [(Vectors.dense([0.0]), 0.0),
    ...      (Vectors.dense([0.4]), 1.0),
    ...      (Vectors.dense([0.5]), 0.0),
    ...      (Vectors.dense([0.6]), 1.0),
    ...      (Vectors.dense([1.0]), 1.0)] * 10,
    ...     ["features", "label"]).repartition(1)
    >>> lr = LogisticRegression()
    >>> grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
    >>> evaluator = BinaryClassificationEvaluator()
    >>> tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator,
    ...     parallelism=1, seed=42)
    >>> tvsModel = tvs.fit(dataset)
    >>> tvsModel.getTrainRatio()
    0.75
    >>> tvsModel.validationMetrics
    [0.5, ...
    >>> path = tempfile.mkdtemp()
    >>> model_path = path + "/model"
    >>> tvsModel.write().save(model_path)
    >>> tvsModelRead = TrainValidationSplitModel.read().load(model_path)
    >>> tvsModelRead.validationMetrics
    [0.5, ...
    >>> evaluator.evaluate(tvsModel.transform(dataset))
    0.833...
    >>> evaluator.evaluate(tvsModelRead.transform(dataset))
    0.833...
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        estimator: Optional[Estimator] = None,
        estimatorParamMaps: Optional[List["ParamMap"]] = None,
        evaluator: Optional[Evaluator] = None,
        trainRatio: float = 0.75,
        parallelism: int = 1,
        collectSubModels: bool = False,
        seed: Optional[int] = None,
    ) -> None:
        """
        __init__(self, \\*, estimator=None, estimatorParamMaps=None, evaluator=None, \
                 trainRatio=0.75, parallelism=1, collectSubModels=False, seed=None)
        """
        super(TrainValidationSplit, self).__init__()
        self._setDefault(parallelism=1)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @since("2.0.0")
    @keyword_only
    def setParams(
        self,
        *,
        estimator: Optional[Estimator] = None,
        estimatorParamMaps: Optional[List["ParamMap"]] = None,
        evaluator: Optional[Evaluator] = None,
        trainRatio: float = 0.75,
        parallelism: int = 1,
        collectSubModels: bool = False,
        seed: Optional[int] = None,
    ) -> "TrainValidationSplit":
        """
        setParams(self, \\*, estimator=None, estimatorParamMaps=None, evaluator=None, \
                  trainRatio=0.75, parallelism=1, collectSubModels=False, seed=None):
        Sets params for the train validation split.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setEstimator(self, value: Estimator) -> "TrainValidationSplit":
        """
        Sets the value of :py:attr:`estimator`.
        """
        return self._set(estimator=value)

    @since("2.0.0")
    def setEstimatorParamMaps(self, value: List["ParamMap"]) -> "TrainValidationSplit":
        """
        Sets the value of :py:attr:`estimatorParamMaps`.
        """
        return self._set(estimatorParamMaps=value)

    @since("2.0.0")
    def setEvaluator(self, value: Evaluator) -> "TrainValidationSplit":
        """
        Sets the value of :py:attr:`evaluator`.
        """
        return self._set(evaluator=value)

    @since("2.0.0")
    def setTrainRatio(self, value: float) -> "TrainValidationSplit":
        """
        Sets the value of :py:attr:`trainRatio`.
        """
        return self._set(trainRatio=value)

    def setSeed(self, value: int) -> "TrainValidationSplit":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def setParallelism(self, value: int) -> "TrainValidationSplit":
        """
        Sets the value of :py:attr:`parallelism`.
        """
        return self._set(parallelism=value)

    def setCollectSubModels(self, value: bool) -> "TrainValidationSplit":
        """
        Sets the value of :py:attr:`collectSubModels`.
        """
        return self._set(collectSubModels=value)

    def _fit(self, dataset: DataFrame) -> "TrainValidationSplitModel":
        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        eva = self.getOrDefault(self.evaluator)
        tRatio = self.getOrDefault(self.trainRatio)
        seed = self.getOrDefault(self.seed)
        randCol = self.uid + "_rand"
        df = dataset.select("*", rand(seed).alias(randCol))
        condition = df[randCol] >= tRatio
        validation = df.filter(condition).cache()
        train = df.filter(~condition).cache()

        subModels = None
        collectSubModelsParam = self.getCollectSubModels()
        if collectSubModelsParam:
            subModels = [None for i in range(numModels)]

        tasks = map(
            inheritable_thread_target,
            _parallelFitTasks(est, train, eva, validation, epm, collectSubModelsParam),
        )
        pool = ThreadPool(processes=min(self.getParallelism(), numModels))
        metrics = [None] * numModels
        for j, metric, subModel in pool.imap_unordered(lambda f: f(), tasks):
            metrics[j] = metric
            if collectSubModelsParam:
                assert subModels is not None
                subModels[j] = subModel

        train.unpersist()
        validation.unpersist()

        if eva.isLargerBetter():
            bestIndex = np.argmax(cast(List[float], metrics))
        else:
            bestIndex = np.argmin(cast(List[float], metrics))
        bestModel = est.fit(dataset, epm[bestIndex])
        return self._copyValues(
            TrainValidationSplitModel(
                bestModel,
                cast(List[float], metrics),
                subModels,  # type: ignore[arg-type]
            )
        )

    def copy(self, extra: Optional["ParamMap"] = None) -> "TrainValidationSplit":
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies creates a deep copy of
        the embedded paramMap, and copies the embedded and extra parameters over.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        extra : dict, optional
            Extra parameters to copy to the new instance

        Returns
        -------
        :py:class:`TrainValidationSplit`
            Copy of this instance
        """
        if extra is None:
            extra = dict()
        newTVS = Params.copy(self, extra)
        if self.isSet(self.estimator):
            newTVS.setEstimator(self.getEstimator().copy(extra))
        # estimatorParamMaps remain the same
        if self.isSet(self.evaluator):
            newTVS.setEvaluator(self.getEvaluator().copy(extra))
        return newTVS

    @since("2.3.0")
    def write(self) -> MLWriter:
        """Returns an MLWriter instance for this ML instance."""
        if _ValidatorSharedReadWrite.is_java_convertible(self):
            return JavaMLWriter(self)  # type: ignore[arg-type]
        return TrainValidationSplitWriter(self)

    @classmethod
    @since("2.3.0")
    def read(cls) -> TrainValidationSplitReader:
        """Returns an MLReader instance for this class."""
        return TrainValidationSplitReader(cls)

    @classmethod
    def _from_java(cls, java_stage: "JavaObject") -> "TrainValidationSplit":
        """
        Given a Java TrainValidationSplit, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        estimator, epms, evaluator = super(TrainValidationSplit, cls)._from_java_impl(java_stage)
        trainRatio = java_stage.getTrainRatio()
        seed = java_stage.getSeed()
        parallelism = java_stage.getParallelism()
        collectSubModels = java_stage.getCollectSubModels()
        # Create a new instance of this stage.
        py_stage = cls(
            estimator=estimator,
            estimatorParamMaps=epms,
            evaluator=evaluator,
            trainRatio=trainRatio,
            seed=seed,
            parallelism=parallelism,
            collectSubModels=collectSubModels,
        )
        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self) -> "JavaObject":
        """
        Transfer this instance to a Java TrainValidationSplit. Used for ML persistence.

        Returns
        -------
        py4j.java_gateway.JavaObject
            Java object equivalent to this instance.
        """

        estimator, epms, evaluator = super(TrainValidationSplit, self)._to_java_impl()

        _java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.tuning.TrainValidationSplit", self.uid
        )
        _java_obj.setEstimatorParamMaps(epms)
        _java_obj.setEvaluator(evaluator)
        _java_obj.setEstimator(estimator)
        _java_obj.setTrainRatio(self.getTrainRatio())
        _java_obj.setSeed(self.getSeed())
        _java_obj.setParallelism(self.getParallelism())
        _java_obj.setCollectSubModels(self.getCollectSubModels())
        return _java_obj


class TrainValidationSplitModel(
    Model, _TrainValidationSplitParams, MLReadable["TrainValidationSplitModel"], MLWritable
):
    """
    Model from train validation split.

    .. versionadded:: 2.0.0
    """

    def __init__(
        self,
        bestModel: Model,
        validationMetrics: Optional[List[float]] = None,
        subModels: Optional[List[Model]] = None,
    ):
        super(TrainValidationSplitModel, self).__init__()
        #: best model from train validation split
        self.bestModel = bestModel
        #: evaluated validation metrics
        self.validationMetrics = validationMetrics or []
        #: sub models from train validation split
        self.subModels = subModels

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return self.bestModel.transform(dataset)

    def copy(self, extra: Optional["ParamMap"] = None) -> "TrainValidationSplitModel":
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies the underlying bestModel,
        creates a deep copy of the embedded paramMap, and
        copies the embedded and extra parameters over.
        And, this creates a shallow copy of the validationMetrics.
        It does not copy the extra Params into the subModels.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        extra : dict, optional
            Extra parameters to copy to the new instance

        Returns
        -------
        :py:class:`TrainValidationSplitModel`
            Copy of this instance
        """
        if extra is None:
            extra = dict()
        bestModel = self.bestModel.copy(extra)
        validationMetrics = list(self.validationMetrics)
        assert self.subModels is not None
        subModels = [model.copy() for model in self.subModels]
        return self._copyValues(
            TrainValidationSplitModel(bestModel, validationMetrics, subModels), extra=extra
        )

    @since("2.3.0")
    def write(self) -> MLWriter:
        """Returns an MLWriter instance for this ML instance."""
        if _ValidatorSharedReadWrite.is_java_convertible(self):
            return JavaMLWriter(self)  # type: ignore[arg-type]
        return TrainValidationSplitModelWriter(self)

    @classmethod
    @since("2.3.0")
    def read(cls) -> TrainValidationSplitModelReader:
        """Returns an MLReader instance for this class."""
        return TrainValidationSplitModelReader(cls)

    @classmethod
    def _from_java(cls, java_stage: "JavaObject") -> "TrainValidationSplitModel":
        """
        Given a Java TrainValidationSplitModel, create and return a Python wrapper of it.
        Used for ML persistence.
        """

        # Load information from java_stage to the instance.
        sc = SparkContext._active_spark_context
        assert sc is not None

        bestModel: Model = JavaParams._from_java(java_stage.bestModel())
        validationMetrics = _java2py(sc, java_stage.validationMetrics())
        estimator, epms, evaluator = super(TrainValidationSplitModel, cls)._from_java_impl(
            java_stage
        )
        # Create a new instance of this stage.
        py_stage = cls(bestModel=bestModel, validationMetrics=validationMetrics)
        params = {
            "evaluator": evaluator,
            "estimator": estimator,
            "estimatorParamMaps": epms,
            "trainRatio": java_stage.getTrainRatio(),
            "seed": java_stage.getSeed(),
        }
        for param_name, param_val in params.items():
            py_stage = py_stage._set(**{param_name: param_val})

        if java_stage.hasSubModels():
            py_stage.subModels = [
                JavaParams._from_java(sub_model) for sub_model in java_stage.subModels()
            ]

        py_stage._resetUid(java_stage.uid())
        return py_stage

    def _to_java(self) -> "JavaObject":
        """
        Transfer this instance to a Java TrainValidationSplitModel. Used for ML persistence.

        Returns
        -------
        py4j.java_gateway.JavaObject
            Java object equivalent to this instance.
        """

        sc = SparkContext._active_spark_context
        assert sc is not None

        _java_obj = JavaParams._new_java_obj(
            "org.apache.spark.ml.tuning.TrainValidationSplitModel",
            self.uid,
            cast(JavaParams, self.bestModel)._to_java(),
            _py2java(sc, self.validationMetrics),
        )
        estimator, epms, evaluator = super(TrainValidationSplitModel, self)._to_java_impl()

        params = {
            "evaluator": evaluator,
            "estimator": estimator,
            "estimatorParamMaps": epms,
            "trainRatio": self.getTrainRatio(),
            "seed": self.getSeed(),
        }
        for param_name, param_val in params.items():
            java_param = _java_obj.getParam(param_name)
            pair = java_param.w(param_val)
            _java_obj.set(pair)

        if self.subModels is not None:
            java_sub_models = [
                cast(JavaParams, sub_model)._to_java() for sub_model in self.subModels
            ]
            _java_obj.setSubModels(java_sub_models)

        return _java_obj


if __name__ == "__main__":
    import doctest

    from pyspark.sql import SparkSession

    globs = globals().copy()

    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder.master("local[2]").appName("ml.tuning tests").getOrCreate()
    sc = spark.sparkContext
    globs["sc"] = sc
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        sys.exit(-1)

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

from pyspark import SparkContext
from pyspark import since
from pyspark.ml.param import Param, Params
from pyspark.ml.util import keyword_only, MLReadable, MLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.mllib.common import inherit_doc, _py2java, _java2py


class PipelineWrapper(object):
    """
    A pipeline wrapper for :py:class:`Pipeline` and :py:class:`PipelineModel` supports transferring
    the array of pipeline stages between Python side and Scala side.
    """

    def _transfer_stages_to_java(self, py_stages):
        """
        Transforms the parameter of Python stages to a list of Java stages.
        """

        def __transfer_stage_to_java(py_stage):
            py_stage._transfer_params_to_java()
            return py_stage._java_obj

        return [__transfer_stage_to_java(stage) for stage in py_stages]

    @staticmethod
    def __get_class(clazz):
        """
        Loads class from its name.
        """
        parts = clazz.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m

    def _transfer_stages_from_java(self, java_sc, java_stages):
        """
        Transforms the parameter Python stages from a list of Java stages.
        """

        def __transfer_stage_from_java(java_stage):
            stage_name = java_stage.getClass().getName().replace("org.apache.spark", "pyspark")
            # Generate a default new instance from the stage_name class.
            py_stage = self.__get_class(stage_name)()
            # Load information from java_stage to the instance.
            py_stage._java_obj = java_stage
            py_stage._resetUid(_java2py(java_sc, java_stage.uid()))
            py_stage._transfer_params_from_java()
            return py_stage

        return [__transfer_stage_from_java(stage) for stage in java_stages]


@inherit_doc
class Pipeline(PipelineWrapper, JavaEstimator, MLReadable, MLWritable):
    """
    A simple pipeline, which acts as an estimator. A Pipeline consists
    of a sequence of stages, each of which is either an
    :py:class:`Estimator` or a :py:class:`Transformer`. When
    :py:meth:`Pipeline.fit` is called, the stages are executed in
    order. If a stage is an :py:class:`Estimator`, its
    :py:meth:`Estimator.fit` method will be called on the input
    dataset to fit a model. Then the model, which is a transformer,
    will be used to transform the dataset as the input to the next
    stage. If a stage is a :py:class:`Transformer`, its
    :py:meth:`Transformer.transform` method will be called to produce
    the dataset for the next stage. The fitted model from a
    :py:class:`Pipeline` is an :py:class:`PipelineModel`, which
    consists of fitted models and transformers, corresponding to the
    pipeline stages. If there are no stages, the pipeline acts as an
    identity transformer.

    >>> from pyspark.ml.feature import HashingTF
    >>> from pyspark.ml.feature import PCA
    >>> df = sqlContext.createDataFrame([(["a", "b", "c"],), (["c", "d", "e"],)], ["words"])
    >>> hashingTF = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
    >>> pca = PCA(k=2, inputCol="features", outputCol="pca_features")
    >>> pl = Pipeline(stages=[hashingTF, pca])
    >>> model = pl.fit(df)
    >>> transformed = model.transform(df)
    >>> transformed.head().words == ["a", "b", "c"]
    True
    >>> transformed.head().features
    SparseVector(10, {7: 1.0, 8: 1.0, 9: 1.0})
    >>> transformed.head().pca_features
    DenseVector([1.0, 0.5774])
    >>> import tempfile
    >>> path = tempfile.mkdtemp()
    >>> featurePath = path + "/feature-transformer"
    >>> pl.save(featurePath)
    >>> loadedPipeline = Pipeline.load(featurePath)
    >>> loadedPipeline.uid == pl.uid
    True
    >>> len(loadedPipeline.getStages())
    2
    >>> [loadedHT, loadedPCA] = loadedPipeline.getStages()
    >>> type(loadedHT)
    <class 'pyspark.ml.feature.HashingTF'>
    >>> type(loadedPCA)
    <class 'pyspark.ml.feature.PCA'>
    >>> loadedHT.uid == hashingTF.uid
    True
    >>> param = loadedHT.getParam("numFeatures")
    >>> loadedHT.getOrDefault(param) == hashingTF.getOrDefault(param)
    True
    >>> loadedPCA.uid == pca.uid
    True
    >>> loadedPCA.getK() == pca.getK()
    True
    >>> modelPath = path + "/feature-model"
    >>> model.save(modelPath)
    >>> loadedModel = PipelineModel.load(modelPath)
    >>> [hashingTFinModel, pcaModel] = model.stages
    >>> [loadedHTinModel, loadedPCAModel] = loadedModel.stages
    >>> hashingTFinModel.uid == loadedHTinModel.uid
    True
    >>> hashingTFinModel.getOrDefault(param) == loadedHTinModel.getOrDefault(param)
    True
    >>> pcaModel.uid == loadedPCAModel.uid
    True
    >>> pcaModel.pc == loadedPCAModel.pc
    True
    >>> pcaModel.explainedVariance == loadedPCAModel.explainedVariance
    True
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass

    .. versionadded:: 1.3.0
    """

    stages = Param(Params._dummy(), "stages", "pipeline stages")

    @keyword_only
    def __init__(self, stages=None):
        """
        __init__(self, stages=None)
        """
        super(Pipeline, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.Pipeline", self.uid)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.3.0")
    def setParams(self, stages=None):
        """
        setParams(self, stages=None)
        Sets params for Pipeline.
        """
        if stages is None:
            stages = []
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.3.0")
    def setStages(self, value):
        """
        Set pipeline stages.

        :param value: a list of transformers or estimators
        :return: the pipeline instance
        """
        self._paramMap[self.stages] = value
        return self

    @since("1.3.0")
    def getStages(self):
        """
        Get pipeline stages.
        """
        if self.stages in self._paramMap:
            return self._paramMap[self.stages]

    def _transfer_params_to_java(self):
        """
        Transforms the parameter stages to Java stages.
        """
        paramMap = self.extractParamMap()
        assert self.stages in paramMap
        param = self.stages
        value = paramMap[param]

        sc = SparkContext._active_spark_context
        param = self._resolveParam(param)
        java_param = self._java_obj.getParam(param.name)
        gateway = SparkContext._gateway
        jvm = SparkContext._jvm
        stageArray = gateway.new_array(jvm.org.apache.spark.ml.PipelineStage, len(value))

        for idx, java_stage in enumerate(self._transfer_stages_to_java(self.getStages())):
            stageArray[idx] = java_stage

        java_value = _py2java(sc, stageArray)
        self._java_obj.set(java_param.w(java_value))

    def _transfer_params_from_java(self):
        """
        Transforms the parameter stages from the companion Java object.
        """
        sc = SparkContext._active_spark_context
        assert self._java_obj.hasParam(self.stages.name)
        java_param = self._java_obj.getParam(self.stages.name)
        if self._java_obj.isDefined(java_param):
            java_stages = _java2py(sc, self._java_obj.getOrDefault(java_param))
            self._paramMap[self.stages] = self._transfer_stages_from_java(sc, java_stages)
        else:
            self._paramMap[self.stages] = []

    def _create_model(self, java_model):
        return PipelineModel(java_model)


@inherit_doc
class PipelineModel(PipelineWrapper, JavaModel, MLReadable, MLWritable):
    """
    Represents a compiled pipeline with transformers and fitted models.

    .. versionadded:: 1.3.0
    """

    @property
    @since("2.0.0")
    def stages(self):
        """
        Returns stages of the pipeline model.
        """
        sc = SparkContext._active_spark_context
        java_stages = self._call_java("stages")
        py_stages = self._transfer_stages_from_java(sc, java_stages)
        return py_stages


if __name__ == "__main__":
    import doctest
    import pyspark.ml
    import pyspark.ml.feature
    from pyspark.sql import SQLContext
    globs = pyspark.ml.__dict__.copy()
    globs_feature = pyspark.ml.feature.__dict__.copy()
    globs.update(globs_feature)
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.pipeline tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)

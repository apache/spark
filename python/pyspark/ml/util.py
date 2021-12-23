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

import json
import os
import time
import uuid

from pyspark import SparkContext, since
from pyspark.ml.common import inherit_doc
from pyspark.sql import SparkSession
from pyspark.util import VersionUtils


def _jvm():
    """
    Returns the JVM view associated with SparkContext. Must be called
    after SparkContext is initialized.
    """
    jvm = SparkContext._jvm
    if jvm:
        return jvm
    else:
        raise AttributeError("Cannot load _jvm from SparkContext. Is SparkContext initialized?")


class Identifiable:
    """
    Object with a unique ID.
    """

    def __init__(self):
        #: A unique id for the object.
        self.uid = self._randomUID()

    def __repr__(self):
        return self.uid

    @classmethod
    def _randomUID(cls):
        """
        Generate a unique string id for the object. The default implementation
        concatenates the class name, "_", and 12 random hex chars.
        """
        return str(cls.__name__ + "_" + uuid.uuid4().hex[-12:])


@inherit_doc
class BaseReadWrite:
    """
    Base class for MLWriter and MLReader. Stores information about the SparkContext
    and SparkSession.

    .. versionadded:: 2.3.0
    """

    def __init__(self):
        self._sparkSession = None

    def session(self, sparkSession):
        """
        Sets the Spark Session to use for saving/loading.
        """
        self._sparkSession = sparkSession
        return self

    @property
    def sparkSession(self):
        """
        Returns the user-specified Spark Session or the default.
        """
        if self._sparkSession is None:
            self._sparkSession = SparkSession._getActiveSessionOrCreate()
        return self._sparkSession

    @property
    def sc(self):
        """
        Returns the underlying `SparkContext`.
        """
        return self.sparkSession.sparkContext


@inherit_doc
class MLWriter(BaseReadWrite):
    """
    Utility class that can save ML instances.

    .. versionadded:: 2.0.0
    """

    def __init__(self):
        super(MLWriter, self).__init__()
        self.shouldOverwrite = False
        self.optionMap = {}

    def _handleOverwrite(self, path):
        from pyspark.ml.wrapper import JavaWrapper

        _java_obj = JavaWrapper._new_java_obj("org.apache.spark.ml.util.FileSystemOverwrite")
        wrapper = JavaWrapper(_java_obj)
        wrapper._call_java("handleOverwrite", path, True, self.sparkSession._jsparkSession)

    def save(self, path):
        """Save the ML instance to the input path."""
        if self.shouldOverwrite:
            self._handleOverwrite(path)
        self.saveImpl(path)

    def saveImpl(self, path):
        """
        save() handles overwriting and then calls this method.  Subclasses should override this
        method to implement the actual saving of the instance.
        """
        raise NotImplementedError("MLWriter is not yet implemented for type: %s" % type(self))

    def overwrite(self):
        """Overwrites if the output path already exists."""
        self.shouldOverwrite = True
        return self

    def option(self, key, value):
        """
        Adds an option to the underlying MLWriter. See the documentation for the specific model's
        writer for possible options. The option name (key) is case-insensitive.
        """
        self.optionMap[key.lower()] = str(value)
        return self


@inherit_doc
class GeneralMLWriter(MLWriter):
    """
    Utility class that can save ML instances in different formats.

    .. versionadded:: 2.4.0
    """

    def format(self, source):
        """
        Specifies the format of ML export ("pmml", "internal", or the fully qualified class
        name for export).
        """
        self.source = source
        return self


@inherit_doc
class JavaMLWriter(MLWriter):
    """
    (Private) Specialization of :py:class:`MLWriter` for :py:class:`JavaParams` types
    """

    def __init__(self, instance):
        super(JavaMLWriter, self).__init__()
        _java_obj = instance._to_java()
        self._jwrite = _java_obj.write()

    def save(self, path):
        """Save the ML instance to the input path."""
        if not isinstance(path, str):
            raise TypeError("path should be a string, got type %s" % type(path))
        self._jwrite.save(path)

    def overwrite(self):
        """Overwrites if the output path already exists."""
        self._jwrite.overwrite()
        return self

    def option(self, key, value):
        self._jwrite.option(key, value)
        return self

    def session(self, sparkSession):
        """Sets the Spark Session to use for saving."""
        self._jwrite.session(sparkSession._jsparkSession)
        return self


@inherit_doc
class GeneralJavaMLWriter(JavaMLWriter):
    """
    (Private) Specialization of :py:class:`GeneralMLWriter` for :py:class:`JavaParams` types
    """

    def __init__(self, instance):
        super(GeneralJavaMLWriter, self).__init__(instance)

    def format(self, source):
        """
        Specifies the format of ML export ("pmml", "internal", or the fully qualified class
        name for export).
        """
        self._jwrite.format(source)
        return self


@inherit_doc
class MLWritable:
    """
    Mixin for ML instances that provide :py:class:`MLWriter`.

    .. versionadded:: 2.0.0
    """

    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        raise NotImplementedError("MLWritable is not yet implemented for type: %r" % type(self))

    def save(self, path):
        """Save this ML instance to the given path, a shortcut of 'write().save(path)'."""
        self.write().save(path)


@inherit_doc
class JavaMLWritable(MLWritable):
    """
    (Private) Mixin for ML instances that provide :py:class:`JavaMLWriter`.
    """

    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        return JavaMLWriter(self)


@inherit_doc
class GeneralJavaMLWritable(JavaMLWritable):
    """
    (Private) Mixin for ML instances that provide :py:class:`GeneralJavaMLWriter`.
    """

    def write(self):
        """Returns an GeneralMLWriter instance for this ML instance."""
        return GeneralJavaMLWriter(self)


@inherit_doc
class MLReader(BaseReadWrite):
    """
    Utility class that can load ML instances.

    .. versionadded:: 2.0.0
    """

    def __init__(self):
        super(MLReader, self).__init__()

    def load(self, path):
        """Load the ML instance from the input path."""
        raise NotImplementedError("MLReader is not yet implemented for type: %s" % type(self))


@inherit_doc
class JavaMLReader(MLReader):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`JavaParams` types
    """

    def __init__(self, clazz):
        super(JavaMLReader, self).__init__()
        self._clazz = clazz
        self._jread = self._load_java_obj(clazz).read()

    def load(self, path):
        """Load the ML instance from the input path."""
        if not isinstance(path, str):
            raise TypeError("path should be a string, got type %s" % type(path))
        java_obj = self._jread.load(path)
        if not hasattr(self._clazz, "_from_java"):
            raise NotImplementedError(
                "This Java ML type cannot be loaded into Python currently: %r" % self._clazz
            )
        return self._clazz._from_java(java_obj)

    def session(self, sparkSession):
        """Sets the Spark Session to use for loading."""
        self._jread.session(sparkSession._jsparkSession)
        return self

    @classmethod
    def _java_loader_class(cls, clazz):
        """
        Returns the full class name of the Java ML instance. The default
        implementation replaces "pyspark" by "org.apache.spark" in
        the Python full class name.
        """
        java_package = clazz.__module__.replace("pyspark", "org.apache.spark")
        if clazz.__name__ in ("Pipeline", "PipelineModel"):
            # Remove the last package name "pipeline" for Pipeline and PipelineModel.
            java_package = ".".join(java_package.split(".")[0:-1])
        return java_package + "." + clazz.__name__

    @classmethod
    def _load_java_obj(cls, clazz):
        """Load the peer Java object of the ML instance."""
        java_class = cls._java_loader_class(clazz)
        java_obj = _jvm()
        for name in java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj


@inherit_doc
class MLReadable:
    """
    Mixin for instances that provide :py:class:`MLReader`.

    .. versionadded:: 2.0.0
    """

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        raise NotImplementedError("MLReadable.read() not implemented for type: %r" % cls)

    @classmethod
    def load(cls, path):
        """Reads an ML instance from the input path, a shortcut of `read().load(path)`."""
        return cls.read().load(path)


@inherit_doc
class JavaMLReadable(MLReadable):
    """
    (Private) Mixin for instances that provide JavaMLReader.
    """

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaMLReader(cls)


@inherit_doc
class DefaultParamsWritable(MLWritable):
    """
    Helper trait for making simple :py:class:`Params` types writable.  If a :py:class:`Params`
    class stores all data as :py:class:`Param` values, then extending this trait will provide
    a default implementation of writing saved instances of the class.
    This only handles simple :py:class:`Param` types; e.g., it will not handle
    :py:class:`pyspark.sql.DataFrame`. See :py:class:`DefaultParamsReadable`, the counterpart
    to this class.

    .. versionadded:: 2.3.0
    """

    def write(self):
        """Returns a DefaultParamsWriter instance for this class."""
        from pyspark.ml.param import Params

        if isinstance(self, Params):
            return DefaultParamsWriter(self)
        else:
            raise TypeError(
                "Cannot use DefaultParamsWritable with type %s because it does not "
                + " extend Params.",
                type(self),
            )


@inherit_doc
class DefaultParamsWriter(MLWriter):
    """
    Specialization of :py:class:`MLWriter` for :py:class:`Params` types

    Class for writing Estimators and Transformers whose parameters are JSON-serializable.

    .. versionadded:: 2.3.0
    """

    def __init__(self, instance):
        super(DefaultParamsWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path):
        DefaultParamsWriter.saveMetadata(self.instance, path, self.sc)

    @staticmethod
    def extractJsonParams(instance, skipParams):
        paramMap = instance.extractParamMap()
        jsonParams = {
            param.name: value for param, value in paramMap.items() if param.name not in skipParams
        }
        return jsonParams

    @staticmethod
    def saveMetadata(instance, path, sc, extraMetadata=None, paramMap=None):
        """
        Saves metadata + Params to: path + "/metadata"

        - class
        - timestamp
        - sparkVersion
        - uid
        - paramMap
        - defaultParamMap (since 2.4.0)
        - (optionally, extra metadata)

        Parameters
        ----------
        extraMetadata : dict, optional
            Extra metadata to be saved at same level as uid, paramMap, etc.
        paramMap : dict, optional
            If given, this is saved in the "paramMap" field.
        """
        metadataPath = os.path.join(path, "metadata")
        metadataJson = DefaultParamsWriter._get_metadata_to_save(
            instance, sc, extraMetadata, paramMap
        )
        sc.parallelize([metadataJson], 1).saveAsTextFile(metadataPath)

    @staticmethod
    def _get_metadata_to_save(instance, sc, extraMetadata=None, paramMap=None):
        """
        Helper for :py:meth:`DefaultParamsWriter.saveMetadata` which extracts the JSON to save.
        This is useful for ensemble models which need to save metadata for many sub-models.

        Notes
        -----
        See :py:meth:`DefaultParamsWriter.saveMetadata` for details on what this includes.
        """
        uid = instance.uid
        cls = instance.__module__ + "." + instance.__class__.__name__

        # User-supplied param values
        params = instance._paramMap
        jsonParams = {}
        if paramMap is not None:
            jsonParams = paramMap
        else:
            for p in params:
                jsonParams[p.name] = params[p]

        # Default param values
        jsonDefaultParams = {}
        for p in instance._defaultParamMap:
            jsonDefaultParams[p.name] = instance._defaultParamMap[p]

        basicMetadata = {
            "class": cls,
            "timestamp": int(round(time.time() * 1000)),
            "sparkVersion": sc.version,
            "uid": uid,
            "paramMap": jsonParams,
            "defaultParamMap": jsonDefaultParams,
        }
        if extraMetadata is not None:
            basicMetadata.update(extraMetadata)
        return json.dumps(basicMetadata, separators=[",", ":"])


@inherit_doc
class DefaultParamsReadable(MLReadable):
    """
    Helper trait for making simple :py:class:`Params` types readable.
    If a :py:class:`Params` class stores all data as :py:class:`Param` values,
    then extending this trait will provide a default implementation of reading saved
    instances of the class. This only handles simple :py:class:`Param` types;
    e.g., it will not handle :py:class:`pyspark.sql.DataFrame`. See
    :py:class:`DefaultParamsWritable`, the counterpart to this class.

    .. versionadded:: 2.3.0
    """

    @classmethod
    def read(cls):
        """Returns a DefaultParamsReader instance for this class."""
        return DefaultParamsReader(cls)


@inherit_doc
class DefaultParamsReader(MLReader):
    """
    Specialization of :py:class:`MLReader` for :py:class:`Params` types

    Default :py:class:`MLReader` implementation for transformers and estimators that
    contain basic (json-serializable) params and no data. This will not handle
    more complex params or types with data (e.g., models with coefficients).

    .. versionadded:: 2.3.0
    """

    def __init__(self, cls):
        super(DefaultParamsReader, self).__init__()
        self.cls = cls

    @staticmethod
    def __get_class(clazz):
        """
        Loads Python class from its name.
        """
        parts = clazz.split(".")
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m

    def load(self, path):
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        py_type = DefaultParamsReader.__get_class(metadata["class"])
        instance = py_type()
        instance._resetUid(metadata["uid"])
        DefaultParamsReader.getAndSetParams(instance, metadata)
        return instance

    @staticmethod
    def loadMetadata(path, sc, expectedClassName=""):
        """
        Load metadata saved using :py:meth:`DefaultParamsWriter.saveMetadata`

        Parameters
        ----------
        path : str
        sc : :py:class:`pyspark.SparkContext`
        expectedClassName : str, optional
            If non empty, this is checked against the loaded metadata.
        """
        metadataPath = os.path.join(path, "metadata")
        metadataStr = sc.textFile(metadataPath, 1).first()
        loadedVals = DefaultParamsReader._parseMetaData(metadataStr, expectedClassName)
        return loadedVals

    @staticmethod
    def _parseMetaData(metadataStr, expectedClassName=""):
        """
        Parse metadata JSON string produced by :py:meth`DefaultParamsWriter._get_metadata_to_save`.
        This is a helper function for :py:meth:`DefaultParamsReader.loadMetadata`.

        Parameters
        ----------
        metadataStr : str
            JSON string of metadata
        expectedClassName : str, optional
            If non empty, this is checked against the loaded metadata.
        """
        metadata = json.loads(metadataStr)
        className = metadata["class"]
        if len(expectedClassName) > 0:
            assert className == expectedClassName, (
                "Error loading metadata: Expected "
                + "class name {} but found class name {}".format(expectedClassName, className)
            )
        return metadata

    @staticmethod
    def getAndSetParams(instance, metadata, skipParams=None):
        """
        Extract Params from metadata, and set them in the instance.
        """
        # Set user-supplied param values
        for paramName in metadata["paramMap"]:
            param = instance.getParam(paramName)
            if skipParams is None or paramName not in skipParams:
                paramValue = metadata["paramMap"][paramName]
                instance.set(param, paramValue)

        # Set default param values
        majorAndMinorVersions = VersionUtils.majorMinorVersion(metadata["sparkVersion"])
        major = majorAndMinorVersions[0]
        minor = majorAndMinorVersions[1]

        # For metadata file prior to Spark 2.4, there is no default section.
        if major > 2 or (major == 2 and minor >= 4):
            assert "defaultParamMap" in metadata, (
                "Error loading metadata: Expected " + "`defaultParamMap` section not found"
            )

            for paramName in metadata["defaultParamMap"]:
                paramValue = metadata["defaultParamMap"][paramName]
                instance._setDefault(**{paramName: paramValue})

    @staticmethod
    def isPythonParamsInstance(metadata):
        return metadata["class"].startswith("pyspark.ml.")

    @staticmethod
    def loadParamsInstance(path, sc):
        """
        Load a :py:class:`Params` instance from the given path, and return it.
        This assumes the instance inherits from :py:class:`MLReadable`.
        """
        metadata = DefaultParamsReader.loadMetadata(path, sc)
        if DefaultParamsReader.isPythonParamsInstance(metadata):
            pythonClassName = metadata["class"]
        else:
            pythonClassName = metadata["class"].replace("org.apache.spark", "pyspark")
        py_type = DefaultParamsReader.__get_class(pythonClassName)
        instance = py_type.load(path)
        return instance


@inherit_doc
class HasTrainingSummary:
    """
    Base class for models that provides Training summary.

    .. versionadded:: 3.0.0
    """

    @property
    @since("2.1.0")
    def hasSummary(self):
        """
        Indicates whether a training summary exists for this model
        instance.
        """
        return self._call_java("hasSummary")

    @property
    @since("2.1.0")
    def summary(self):
        """
        Gets summary of the model trained on the training set. An exception is thrown if
        no summary exists.
        """
        return self._call_java("summary")


class MetaAlgorithmReadWrite:
    @staticmethod
    def isMetaEstimator(pyInstance):
        from pyspark.ml import Estimator, Pipeline
        from pyspark.ml.tuning import _ValidatorParams
        from pyspark.ml.classification import OneVsRest

        return (
            isinstance(pyInstance, Pipeline)
            or isinstance(pyInstance, OneVsRest)
            or (isinstance(pyInstance, Estimator) and isinstance(pyInstance, _ValidatorParams))
        )

    @staticmethod
    def getAllNestedStages(pyInstance):
        from pyspark.ml import Pipeline, PipelineModel
        from pyspark.ml.tuning import _ValidatorParams
        from pyspark.ml.classification import OneVsRest, OneVsRestModel

        # TODO: We need to handle `RFormulaModel.pipelineModel` here after Pyspark RFormulaModel
        #  support pipelineModel property.
        if isinstance(pyInstance, Pipeline):
            pySubStages = pyInstance.getStages()
        elif isinstance(pyInstance, PipelineModel):
            pySubStages = pyInstance.stages
        elif isinstance(pyInstance, _ValidatorParams):
            raise ValueError("PySpark does not support nested validator.")
        elif isinstance(pyInstance, OneVsRest):
            pySubStages = [pyInstance.getClassifier()]
        elif isinstance(pyInstance, OneVsRestModel):
            pySubStages = [pyInstance.getClassifier()] + pyInstance.models
        else:
            pySubStages = []

        nestedStages = []
        for pySubStage in pySubStages:
            nestedStages.extend(MetaAlgorithmReadWrite.getAllNestedStages(pySubStage))

        return [pyInstance] + nestedStages

    @staticmethod
    def getUidMap(instance):
        nestedStages = MetaAlgorithmReadWrite.getAllNestedStages(instance)
        uidMap = {stage.uid: stage for stage in nestedStages}
        if len(nestedStages) != len(uidMap):
            raise RuntimeError(
                f"{instance.__class__.__module__}.{instance.__class__.__name__}"
                f".load found a compound estimator with stages with duplicate "
                f"UIDs. List of UIDs: {list(uidMap.keys())}."
            )
        return uidMap

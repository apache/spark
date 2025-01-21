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
import functools
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    cast,
    TYPE_CHECKING,
    Union,
)

from pyspark import since
from pyspark.ml.common import inherit_doc
from pyspark.sql import SparkSession
from pyspark.sql.utils import is_remote
from pyspark.util import VersionUtils

if TYPE_CHECKING:
    from py4j.java_gateway import JavaGateway, JavaObject
    from pyspark.ml._typing import PipelineStage
    from pyspark.ml.base import Params
    from pyspark.ml.wrapper import JavaWrapper
    from pyspark.core.context import SparkContext
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.ml.wrapper import JavaWrapper, JavaEstimator
    from pyspark.ml.evaluation import JavaEvaluator

T = TypeVar("T")
RW = TypeVar("RW", bound="BaseReadWrite")
W = TypeVar("W", bound="MLWriter")
JW = TypeVar("JW", bound="JavaMLWriter")
RL = TypeVar("RL", bound="MLReadable")
JR = TypeVar("JR", bound="JavaMLReader")

FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def try_remote_intermediate_result(f: FuncT) -> FuncT:
    """Mark the function/property that returns the intermediate result of the remote call.
    Eg, model.summary"""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            return f"{self._java_obj}.{f.__name__}"
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_attribute_relation(f: FuncT) -> FuncT:
    """Mark the function/property that returns a Relation.
    Eg, model.summary.roc"""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper", *args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            import pyspark.sql.connect.proto as pb2
            from pyspark.ml.connect.util import _extract_id_methods
            from pyspark.ml.connect.serialize import serialize

            # The attribute returns a dataframe, we need to wrap it
            # in the AttributeRelation
            from pyspark.ml.connect.proto import AttributeRelation
            from pyspark.sql.connect.session import SparkSession
            from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

            session = SparkSession.getActiveSession()
            assert session is not None

            assert isinstance(self._java_obj, str)

            methods, obj_ref = _extract_id_methods(self._java_obj)
            methods.append(
                pb2.Fetch.Method(method=f.__name__, args=serialize(session.client, *args))
            )
            plan = AttributeRelation(obj_ref, methods)
            return ConnectDataFrame(plan, session)
        else:
            return f(self, *args, **kwargs)

    return cast(FuncT, wrapped)


def try_remote_fit(f: FuncT) -> FuncT:
    """Mark the function that fits a model."""

    @functools.wraps(f)
    def wrapped(self: "JavaEstimator", dataset: "ConnectDataFrame") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            import pyspark.sql.connect.proto as pb2
            from pyspark.ml.connect.serialize import serialize_ml_params, deserialize

            client = dataset.sparkSession.client
            input = dataset._plan.plan(client)
            assert isinstance(self._java_obj, str)
            estimator = pb2.MlOperator(
                name=self._java_obj, uid=self.uid, type=pb2.MlOperator.ESTIMATOR
            )
            command = pb2.Command()
            command.ml_command.fit.CopyFrom(
                pb2.MlCommand.Fit(
                    estimator=estimator,
                    params=serialize_ml_params(self, client),
                    dataset=input,
                )
            )
            (_, properties, _) = client.execute_command(command)
            model_info = deserialize(properties)
            client.add_ml_cache(model_info.obj_ref.id)
            return model_info.obj_ref.id
        else:
            return f(self, dataset)

    return cast(FuncT, wrapped)


def try_remote_transform_relation(f: FuncT) -> FuncT:
    """Mark the function/property that returns a relation for model transform."""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper", dataset: "ConnectDataFrame") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml import Model, Transformer
            from pyspark.sql.connect.session import SparkSession
            from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
            from pyspark.ml.connect.serialize import serialize_ml_params

            session = SparkSession.getActiveSession()
            assert session is not None
            # Model is also a Transformer, so we much match Model first
            if isinstance(self, Model):
                params = serialize_ml_params(self, session.client)
                from pyspark.ml.connect.proto import TransformerRelation

                assert isinstance(self._java_obj, str)
                return ConnectDataFrame(
                    TransformerRelation(
                        child=dataset._plan, name=self._java_obj, ml_params=params, is_model=True
                    ),
                    session,
                )
            elif isinstance(self, Transformer):
                params = serialize_ml_params(self, session.client)
                from pyspark.ml.connect.proto import TransformerRelation

                assert isinstance(self._java_obj, str)
                return ConnectDataFrame(
                    TransformerRelation(
                        child=dataset._plan,
                        name=self._java_obj,
                        ml_params=params,
                        uid=self.uid,
                        is_model=False,
                    ),
                    session,
                )
            else:
                raise RuntimeError(f"Unsupported {self}")
        else:
            return f(self, dataset)

    return cast(FuncT, wrapped)


def try_remote_call(f: FuncT) -> FuncT:
    """Mark the function/property for the remote call.
    Eg, model.coefficients"""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper", name: str, *args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            # Launch a remote call if possible
            import pyspark.sql.connect.proto as pb2
            from pyspark.sql.connect.session import SparkSession
            from pyspark.ml.connect.util import _extract_id_methods
            from pyspark.ml.connect.serialize import serialize, deserialize

            session = SparkSession.getActiveSession()
            assert session is not None
            assert isinstance(self._java_obj, str)
            methods, obj_ref = _extract_id_methods(self._java_obj)
            methods.append(pb2.Fetch.Method(method=name, args=serialize(session.client, *args)))
            command = pb2.Command()
            command.ml_command.fetch.CopyFrom(
                pb2.Fetch(obj_ref=pb2.ObjectRef(id=obj_ref), methods=methods)
            )
            (_, properties, _) = session.client.execute_command(command)
            ml_command_result = properties["ml_command_result"]
            if ml_command_result.HasField("summary"):
                summary = ml_command_result.summary
                session.client.add_ml_cache(summary)
                return summary
            else:
                return deserialize(properties)
        else:
            return f(self, name, *args)

    return cast(FuncT, wrapped)


def try_remote_del(f: FuncT) -> FuncT:
    """Mark the function/property to delete a model on the server side."""

    @functools.wraps(f)
    def wrapped(self: "JavaWrapper") -> Any:
        try:
            in_remote = is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ
        except Exception:
            return

        if in_remote:
            # Delete the model if possible
            model_id = self._java_obj
            if model_id is not None and "." not in model_id:
                try:
                    from pyspark.sql.connect.session import SparkSession

                    session = SparkSession.getActiveSession()
                    if session is not None:
                        session.client.remove_ml_cache(model_id)
                        return
                except Exception:
                    # SparkSession's down.
                    return
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_return_java_class(f: FuncT) -> FuncT:
    """Mark the function/property that returns none."""

    @functools.wraps(f)
    def wrapped(java_class: str, *args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            return java_class
        else:
            return f(java_class, *args)

    return cast(FuncT, wrapped)


def try_remote_write(f: FuncT) -> FuncT:
    """Mark the function that write an estimator/model or evaluator"""

    @functools.wraps(f)
    def wrapped(self: "JavaMLWritable") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml.connect.readwrite import RemoteMLWriter

            return RemoteMLWriter(self)
        else:
            return f(self)

    return cast(FuncT, wrapped)


def try_remote_read(f: FuncT) -> FuncT:
    """Mark the function to read an estimator/model or evaluator"""

    @functools.wraps(f)
    def wrapped(cls: Type["JavaMLReadable"]) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml.connect.readwrite import RemoteMLReader

            return RemoteMLReader(cls)
        else:
            return f(cls)

    return cast(FuncT, wrapped)


def try_remote_intercept(f: FuncT) -> FuncT:
    """Mark the function/property that returns none."""

    @functools.wraps(f)
    def wrapped(java_class: str, *args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            return None
        else:
            return f(java_class, *args)

    return cast(FuncT, wrapped)


def try_remote_not_supporting(f: FuncT) -> FuncT:
    """Mark the function/property that has not been supported yet"""

    @functools.wraps(f)
    def wrapped(*args: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            raise NotImplementedError("")
        else:
            return f(*args)

    return cast(FuncT, wrapped)


def try_remote_evaluate(f: FuncT) -> FuncT:
    """Mark the evaluate function in Evaluator."""

    @functools.wraps(f)
    def wrapped(self: "JavaEvaluator", dataset: "ConnectDataFrame") -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            import pyspark.sql.connect.proto as pb2
            from pyspark.ml.connect.serialize import serialize_ml_params, deserialize

            client = dataset.sparkSession.client
            input = dataset._plan.plan(client)
            assert isinstance(self._java_obj, str)
            evaluator = pb2.MlOperator(
                name=self._java_obj, uid=self.uid, type=pb2.MlOperator.EVALUATOR
            )
            command = pb2.Command()
            command.ml_command.evaluate.CopyFrom(
                pb2.MlCommand.Evaluate(
                    evaluator=evaluator,
                    params=serialize_ml_params(self, client),
                    dataset=input,
                )
            )
            (_, properties, _) = client.execute_command(command)
            return deserialize(properties)
        else:
            return f(self, dataset)

    return cast(FuncT, wrapped)


def _jvm() -> "JavaGateway":
    """
    Returns the JVM view associated with SparkContext. Must be called
    after SparkContext is initialized.
    """
    from pyspark.core.context import SparkContext

    jvm = SparkContext._jvm
    if jvm:
        return jvm
    else:
        raise AttributeError("Cannot load _jvm from SparkContext. Is SparkContext initialized?")


class Identifiable:
    """
    Object with a unique ID.
    """

    def __init__(self) -> None:
        #: A unique id for the object.
        self.uid = self._randomUID()

    def __repr__(self) -> str:
        return self.uid

    @classmethod
    def _randomUID(cls) -> str:
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

    def __init__(self) -> None:
        self._sparkSession: Optional[SparkSession] = None

    def session(self: RW, sparkSession: SparkSession) -> RW:
        """
        Sets the Spark Session to use for saving/loading.
        """
        self._sparkSession = sparkSession
        return self

    @property
    def sparkSession(self) -> SparkSession:
        """
        Returns the user-specified Spark Session or the default.
        """
        if self._sparkSession is None:
            self._sparkSession = SparkSession._getActiveSessionOrCreate()
        assert self._sparkSession is not None
        return self._sparkSession

    @property
    def sc(self) -> "SparkContext":
        """
        Returns the underlying `SparkContext`.
        """
        assert self.sparkSession is not None
        return self.sparkSession.sparkContext


@inherit_doc
class MLWriter(BaseReadWrite):
    """
    Utility class that can save ML instances.

    .. versionadded:: 2.0.0
    """

    def __init__(self) -> None:
        super(MLWriter, self).__init__()
        self.shouldOverwrite: bool = False
        self.optionMap: Dict[str, Any] = {}

    def _handleOverwrite(self, path: str) -> None:
        from pyspark.ml.wrapper import JavaWrapper

        _java_obj = JavaWrapper._new_java_obj("org.apache.spark.ml.util.FileSystemOverwrite")
        wrapper = JavaWrapper(_java_obj)
        wrapper._call_java("handleOverwrite", path, True, self.sparkSession._jsparkSession)

    def save(self, path: str) -> None:
        """Save the ML instance to the input path."""
        if self.shouldOverwrite:
            self._handleOverwrite(path)
        self.saveImpl(path)

    def saveImpl(self, path: str) -> None:
        """
        save() handles overwriting and then calls this method.  Subclasses should override this
        method to implement the actual saving of the instance.
        """
        raise NotImplementedError("MLWriter is not yet implemented for type: %s" % type(self))

    def overwrite(self) -> "MLWriter":
        """Overwrites if the output path already exists."""
        self.shouldOverwrite = True
        return self

    def option(self, key: str, value: Any) -> "MLWriter":
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

    def format(self, source: str) -> "GeneralMLWriter":
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

    _jwrite: "JavaObject"

    def __init__(self, instance: "JavaMLWritable"):
        super(JavaMLWriter, self).__init__()
        _java_obj = instance._to_java()  # type: ignore[attr-defined]
        self._jwrite = _java_obj.write()

    def save(self, path: str) -> None:
        """Save the ML instance to the input path."""
        if not isinstance(path, str):
            raise TypeError("path should be a string, got type %s" % type(path))
        self._jwrite.save(path)

    def overwrite(self) -> "JavaMLWriter":
        """Overwrites if the output path already exists."""
        self._jwrite.overwrite()
        return self

    def option(self, key: str, value: str) -> "JavaMLWriter":
        self._jwrite.option(key, value)
        return self

    def session(self, sparkSession: SparkSession) -> "JavaMLWriter":
        """Sets the Spark Session to use for saving."""
        self._jwrite.session(sparkSession._jsparkSession)
        return self


@inherit_doc
class GeneralJavaMLWriter(JavaMLWriter):
    """
    (Private) Specialization of :py:class:`GeneralMLWriter` for :py:class:`JavaParams` types
    """

    def __init__(self, instance: "JavaMLWritable"):
        super(GeneralJavaMLWriter, self).__init__(instance)

    def format(self, source: str) -> "GeneralJavaMLWriter":
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

    def write(self) -> MLWriter:
        """Returns an MLWriter instance for this ML instance."""
        raise NotImplementedError("MLWritable is not yet implemented for type: %r" % type(self))

    def save(self, path: str) -> None:
        """Save this ML instance to the given path, a shortcut of 'write().save(path)'."""
        self.write().save(path)


@inherit_doc
class JavaMLWritable(MLWritable):
    """
    (Private) Mixin for ML instances that provide :py:class:`JavaMLWriter`.
    """

    @try_remote_write
    def write(self) -> JavaMLWriter:
        """Returns an MLWriter instance for this ML instance."""
        return JavaMLWriter(self)


@inherit_doc
class GeneralJavaMLWritable(JavaMLWritable):
    """
    (Private) Mixin for ML instances that provide :py:class:`GeneralJavaMLWriter`.
    """

    @try_remote_write
    def write(self) -> GeneralJavaMLWriter:
        """Returns an GeneralMLWriter instance for this ML instance."""
        return GeneralJavaMLWriter(self)


@inherit_doc
class MLReader(BaseReadWrite, Generic[RL]):
    """
    Utility class that can load ML instances.

    .. versionadded:: 2.0.0
    """

    def __init__(self) -> None:
        super(MLReader, self).__init__()

    def load(self, path: str) -> RL:
        """Load the ML instance from the input path."""
        raise NotImplementedError("MLReader is not yet implemented for type: %s" % type(self))


@inherit_doc
class JavaMLReader(MLReader[RL]):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`JavaParams` types
    """

    def __init__(self, clazz: Type["JavaMLReadable[RL]"]) -> None:
        super(JavaMLReader, self).__init__()
        self._clazz = clazz
        self._jread = self._load_java_obj(clazz).read()

    def load(self, path: str) -> RL:
        """Load the ML instance from the input path."""
        if not isinstance(path, str):
            raise TypeError("path should be a string, got type %s" % type(path))
        java_obj = self._jread.load(path)
        if not hasattr(self._clazz, "_from_java"):
            raise NotImplementedError(
                "This Java ML type cannot be loaded into Python currently: %r" % self._clazz
            )
        return self._clazz._from_java(java_obj)

    def session(self: JR, sparkSession: SparkSession) -> JR:
        """Sets the Spark Session to use for loading."""
        self._jread.session(sparkSession._jsparkSession)
        return self

    @classmethod
    def _java_loader_class(cls, clazz: Type["JavaMLReadable[RL]"]) -> str:
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
    def _load_java_obj(cls, clazz: Type["JavaMLReadable[RL]"]) -> "JavaObject":
        """Load the peer Java object of the ML instance."""
        java_class = cls._java_loader_class(clazz)
        java_obj = _jvm()
        for name in java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj


@inherit_doc
class MLReadable(Generic[RL]):
    """
    Mixin for instances that provide :py:class:`MLReader`.

    .. versionadded:: 2.0.0
    """

    @classmethod
    def read(cls) -> MLReader[RL]:
        """Returns an MLReader instance for this class."""
        raise NotImplementedError("MLReadable.read() not implemented for type: %r" % cls)

    @classmethod
    def load(cls, path: str) -> RL:
        """Reads an ML instance from the input path, a shortcut of `read().load(path)`."""
        return cls.read().load(path)


@inherit_doc
class JavaMLReadable(MLReadable[RL]):
    """
    (Private) Mixin for instances that provide JavaMLReader.
    """

    @classmethod
    @try_remote_read
    def read(cls) -> JavaMLReader[RL]:
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

    def write(self) -> MLWriter:
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

    def __init__(self, instance: "Params"):
        super(DefaultParamsWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path: str) -> None:
        DefaultParamsWriter.saveMetadata(self.instance, path, self.sparkSession)

    @staticmethod
    def extractJsonParams(instance: "Params", skipParams: Sequence[str]) -> Dict[str, Any]:
        paramMap = instance.extractParamMap()
        jsonParams = {
            param.name: value for param, value in paramMap.items() if param.name not in skipParams
        }
        return jsonParams

    @staticmethod
    def saveMetadata(
        instance: "Params",
        path: str,
        sc: Union["SparkContext", SparkSession],
        extraMetadata: Optional[Dict[str, Any]] = None,
        paramMap: Optional[Dict[str, Any]] = None,
    ) -> None:
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
        spark = sc if isinstance(sc, SparkSession) else SparkSession._getActiveSessionOrCreate()
        spark.createDataFrame([(metadataJson,)], schema=["value"]).coalesce(1).write.text(
            metadataPath
        )

    @staticmethod
    def _get_metadata_to_save(
        instance: "Params",
        sc: Union["SparkContext", SparkSession],
        extraMetadata: Optional[Dict[str, Any]] = None,
        paramMap: Optional[Dict[str, Any]] = None,
    ) -> str:
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
        return json.dumps(basicMetadata, separators=(",", ":"))


@inherit_doc
class DefaultParamsReadable(MLReadable[RL]):
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
    def read(cls) -> "DefaultParamsReader[RL]":
        """Returns a DefaultParamsReader instance for this class."""
        return DefaultParamsReader(cls)


@inherit_doc
class DefaultParamsReader(MLReader[RL]):
    """
    Specialization of :py:class:`MLReader` for :py:class:`Params` types

    Default :py:class:`MLReader` implementation for transformers and estimators that
    contain basic (json-serializable) params and no data. This will not handle
    more complex params or types with data (e.g., models with coefficients).

    .. versionadded:: 2.3.0
    """

    def __init__(self, cls: Type[DefaultParamsReadable[RL]]):
        super(DefaultParamsReader, self).__init__()
        self.cls = cls

    @staticmethod
    def __get_class(clazz: str) -> Type[RL]:
        """
        Loads Python class from its name.
        """
        parts = clazz.split(".")
        module = ".".join(parts[:-1])
        m = __import__(module, fromlist=[parts[-1]])
        return getattr(m, parts[-1])

    def load(self, path: str) -> RL:
        metadata = DefaultParamsReader.loadMetadata(path, self.sparkSession)
        py_type: Type[RL] = DefaultParamsReader.__get_class(metadata["class"])
        instance = py_type()
        cast("Params", instance)._resetUid(metadata["uid"])
        DefaultParamsReader.getAndSetParams(instance, metadata)
        return instance

    @staticmethod
    def loadMetadata(
        path: str,
        sc: Union["SparkContext", SparkSession],
        expectedClassName: str = "",
    ) -> Dict[str, Any]:
        """
        Load metadata saved using :py:meth:`DefaultParamsWriter.saveMetadata`

        Parameters
        ----------
        path : str
        sc : :py:class:`pyspark.SparkContext` or :py:class:`pyspark.sql.SparkSession`
        expectedClassName : str, optional
            If non empty, this is checked against the loaded metadata.
        """
        metadataPath = os.path.join(path, "metadata")
        spark = sc if isinstance(sc, SparkSession) else SparkSession._getActiveSessionOrCreate()
        metadataStr = spark.read.text(metadataPath).first()[0]  # type: ignore[index]
        loadedVals = DefaultParamsReader._parseMetaData(metadataStr, expectedClassName)
        return loadedVals

    @staticmethod
    def _parseMetaData(metadataStr: str, expectedClassName: str = "") -> Dict[str, Any]:
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
    def getAndSetParams(
        instance: RL, metadata: Dict[str, Any], skipParams: Optional[List[str]] = None
    ) -> None:
        """
        Extract Params from metadata, and set them in the instance.
        """
        # Set user-supplied param values
        for paramName in metadata["paramMap"]:
            param = cast("Params", instance).getParam(paramName)
            if skipParams is None or paramName not in skipParams:
                paramValue = metadata["paramMap"][paramName]
                cast("Params", instance).set(param, paramValue)

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
                cast("Params", instance)._setDefault(**{paramName: paramValue})

    @staticmethod
    def isPythonParamsInstance(metadata: Dict[str, Any]) -> bool:
        return metadata["class"].startswith("pyspark.ml.")

    @staticmethod
    def loadParamsInstance(path: str, sc: Union["SparkContext", SparkSession]) -> RL:
        """
        Load a :py:class:`Params` instance from the given path, and return it.
        This assumes the instance inherits from :py:class:`MLReadable`.
        """
        metadata = DefaultParamsReader.loadMetadata(path, sc)
        if DefaultParamsReader.isPythonParamsInstance(metadata):
            pythonClassName = metadata["class"]
        else:
            pythonClassName = metadata["class"].replace("org.apache.spark", "pyspark")
        py_type: Type[RL] = DefaultParamsReader.__get_class(pythonClassName)
        instance = py_type.load(path)
        return instance


@inherit_doc
class HasTrainingSummary(Generic[T]):
    """
    Base class for models that provides Training summary.

    .. versionadded:: 3.0.0
    """

    @property
    @since("2.1.0")
    def hasSummary(self) -> bool:
        """
        Indicates whether a training summary exists for this model
        instance.
        """
        return cast("JavaWrapper", self)._call_java("hasSummary")

    @property
    @since("2.1.0")
    @try_remote_intermediate_result
    def summary(self) -> T:
        """
        Gets summary of the model trained on the training set. An exception is thrown if
        no summary exists.
        """
        return cast("JavaWrapper", self)._call_java("summary")


class MetaAlgorithmReadWrite:
    @staticmethod
    def isMetaEstimator(pyInstance: Any) -> bool:
        from pyspark.ml import Estimator, Pipeline
        from pyspark.ml.tuning import _ValidatorParams
        from pyspark.ml.classification import OneVsRest

        return (
            isinstance(pyInstance, Pipeline)
            or isinstance(pyInstance, OneVsRest)
            or (isinstance(pyInstance, Estimator) and isinstance(pyInstance, _ValidatorParams))
        )

    @staticmethod
    def getAllNestedStages(pyInstance: Any) -> List["Params"]:
        from pyspark.ml import Pipeline, PipelineModel
        from pyspark.ml.tuning import _ValidatorParams
        from pyspark.ml.classification import OneVsRest, OneVsRestModel

        # TODO: We need to handle `RFormulaModel.pipelineModel` here after Pyspark RFormulaModel
        #  support pipelineModel property.
        pySubStages: Sequence["Params"]

        if isinstance(pyInstance, Pipeline):
            pySubStages = pyInstance.getStages()
        elif isinstance(pyInstance, PipelineModel):
            pySubStages = cast(List["PipelineStage"], pyInstance.stages)
        elif isinstance(pyInstance, _ValidatorParams):
            raise ValueError("PySpark does not support nested validator.")
        elif isinstance(pyInstance, OneVsRest):
            pySubStages = [pyInstance.getClassifier()]
        elif isinstance(pyInstance, OneVsRestModel):
            pySubStages = [pyInstance.getClassifier()] + pyInstance.models  # type: ignore[operator]
        else:
            pySubStages = []

        nestedStages = []
        for pySubStage in pySubStages:
            nestedStages.extend(MetaAlgorithmReadWrite.getAllNestedStages(pySubStage))

        return [pyInstance] + nestedStages

    @staticmethod
    def getUidMap(instance: Any) -> Dict[str, "Params"]:
        nestedStages = MetaAlgorithmReadWrite.getAllNestedStages(instance)
        uidMap = {stage.uid: stage for stage in nestedStages}
        if len(nestedStages) != len(uidMap):
            raise RuntimeError(
                f"{instance.__class__.__module__}.{instance.__class__.__name__}"
                f".load found a compound estimator with stages with duplicate "
                f"UIDs. List of UIDs: {list(uidMap.keys())}."
            )
        return uidMap


def try_remote_functions(f: FuncT) -> FuncT:
    """Mark API supported from Spark Connect."""

    @functools.wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if is_remote() and "PYSPARK_NO_NAMESPACE_SHARE" not in os.environ:
            from pyspark.ml.connect import functions

            return getattr(functions, f.__name__)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return cast(FuncT, wrapped)

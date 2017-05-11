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

import sys
import uuid
import warnings

if sys.version > '3':
    basestring = str
    unicode = str

from pyspark import SparkContext, since
from pyspark.ml.common import inherit_doc


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


class Identifiable(object):
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
        Generate a unique unicode id for the object. The default implementation
        concatenates the class name, "_", and 12 random hex chars.
        """
        return unicode(cls.__name__ + "_" + uuid.uuid4().hex[12:])


@inherit_doc
class MLWriter(object):
    """
    Utility class that can save ML instances.

    .. versionadded:: 2.0.0
    """

    def save(self, path):
        """Save the ML instance to the input path."""
        raise NotImplementedError("MLWriter is not yet implemented for type: %s" % type(self))

    def overwrite(self):
        """Overwrites if the output path already exists."""
        raise NotImplementedError("MLWriter is not yet implemented for type: %s" % type(self))

    def context(self, sqlContext):
        """
        Sets the SQL context to use for saving.
        .. note:: Deprecated in 2.1 and will be removed in 2.2, use session instead.
        """
        raise NotImplementedError("MLWriter is not yet implemented for type: %s" % type(self))

    def session(self, sparkSession):
        """Sets the Spark Session to use for saving."""
        raise NotImplementedError("MLWriter is not yet implemented for type: %s" % type(self))


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
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))
        self._jwrite.save(path)

    def overwrite(self):
        """Overwrites if the output path already exists."""
        self._jwrite.overwrite()
        return self

    def context(self, sqlContext):
        """
        Sets the SQL context to use for saving.
        .. note:: Deprecated in 2.1 and will be removed in 2.2, use session instead.
        """
        warnings.warn("Deprecated in 2.1 and will be removed in 2.2, use session instead.")
        self._jwrite.context(sqlContext._ssql_ctx)
        return self

    def session(self, sparkSession):
        """Sets the Spark Session to use for saving."""
        self._jwrite.session(sparkSession._jsparkSession)
        return self


@inherit_doc
class MLWritable(object):
    """
    Mixin for ML instances that provide :py:class:`MLWriter`.

    .. versionadded:: 2.0.0
    """

    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        raise NotImplementedError("MLWritable is not yet implemented for type: %r" % type(self))

    def save(self, path):
        """Save this ML instance to the given path, a shortcut of `write().save(path)`."""
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
class MLReader(object):
    """
    Utility class that can load ML instances.

    .. versionadded:: 2.0.0
    """

    def load(self, path):
        """Load the ML instance from the input path."""
        raise NotImplementedError("MLReader is not yet implemented for type: %s" % type(self))

    def context(self, sqlContext):
        """
        Sets the SQL context to use for loading.
        .. note:: Deprecated in 2.1 and will be removed in 2.2, use session instead.
        """
        raise NotImplementedError("MLReader is not yet implemented for type: %s" % type(self))

    def session(self, sparkSession):
        """Sets the Spark Session to use for loading."""
        raise NotImplementedError("MLReader is not yet implemented for type: %s" % type(self))


@inherit_doc
class JavaMLReader(MLReader):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`JavaParams` types
    """

    def __init__(self, clazz):
        self._clazz = clazz
        self._jread = self._load_java_obj(clazz).read()

    def load(self, path):
        """Load the ML instance from the input path."""
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))
        java_obj = self._jread.load(path)
        if not hasattr(self._clazz, "_from_java"):
            raise NotImplementedError("This Java ML type cannot be loaded into Python currently: %r"
                                      % self._clazz)
        return self._clazz._from_java(java_obj)

    def context(self, sqlContext):
        """
        Sets the SQL context to use for loading.
        .. note:: Deprecated in 2.1 and will be removed in 2.2, use session instead.
        """
        warnings.warn("Deprecated in 2.1 and will be removed in 2.2, use session instead.")
        self._jread.context(sqlContext._ssql_ctx)
        return self

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
class MLReadable(object):
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
class JavaPredictionModel():
    """
    (Private) Java Model for prediction tasks (regression and classification).
    To be mixed in with class:`pyspark.ml.JavaModel`
    """

    @property
    @since("2.1.0")
    def numFeatures(self):
        """
        Returns the number of features the model was trained on. If unknown, returns -1
        """
        return self._call_java("numFeatures")

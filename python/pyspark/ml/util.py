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
from functools import wraps

if sys.version > '3':
    basestring = str

from pyspark import SparkContext, since
from pyspark.mllib.common import inherit_doc


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


def keyword_only(func):
    """
    A decorator that forces keyword arguments in the wrapped method
    and saves actual input keyword arguments in `_input_kwargs`.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 1:
            raise TypeError("Method %s forces keyword arguments." % func.__name__)
        wrapper._input_kwargs = kwargs
        return func(*args, **kwargs)
    return wrapper


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
        Generate a unique id for the object. The default implementation
        concatenates the class name, "_", and 12 random hex chars.
        """
        return cls.__name__ + "_" + uuid.uuid4().hex[12:]


@inherit_doc
class MLWriter(object):
    """
    .. note:: Experimental

    Utility class that can save ML instances.

    .. versionadded:: 2.0.0
    """

    def __init__(self, instance):
        self._jwrite = instance._java_obj.write()

    @since("2.0.0")
    def save(self, path):
        """Saves the ML instances to the input path."""
        self._jwrite.save(path)

    @since("2.0.0")
    def overwrite(self):
        """Overwrites if the output path already exists."""
        self._jwrite.overwrite()
        return self

    @since("2.0.0")
    def context(self, sqlContext):
        """Sets the SQL context to use for saving."""
        self._jwrite.context(sqlContext._ssql_ctx)
        return self


@inherit_doc
class MLWritable(object):
    """
    .. note:: Experimental

    Mixin for ML instances that provide MLWriter through their Scala
    implementation.

    .. versionadded:: 2.0.0
    """

    @since("2.0.0")
    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        return MLWriter(self)

    @since("2.0.0")
    def save(self, path):
        """Save this ML instance to the given path, a shortcut of `write().save(path)`."""
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))
        self._java_obj.save(path)


@inherit_doc
class MLReader(object):
    """
    .. note:: Experimental

    Utility class that can load ML instances.

    .. versionadded:: 2.0.0
    """

    def __init__(self, instance):
        self._instance = instance
        self._jread = instance._java_obj.read()

    @since("2.0.0")
    def load(self, path):
        """Loads the ML component from the input path."""
        java_obj = self._jread.load(path)
        self._instance._java_obj = java_obj
        self._instance.uid = java_obj.uid()
        self._instance._transfer_params_from_java(True)
        return self._instance

    @since("2.0.0")
    def context(self, sqlContext):
        """Sets the SQL context to use for loading."""
        self._jread.context(sqlContext._ssql_ctx)
        return self


@inherit_doc
class MLReadable(object):
    """
    .. note:: Experimental

    Mixin for objects that provide MLReader using its Scala implementation.

    .. versionadded:: 2.0.0
    """

    @classmethod
    def _java_loader_class(cls):
        """
        Returns the full class name of the Java loader. The default
        implementation replaces "pyspark" by "org.apache.spark" in
        the Python full class name.
        """
        java_package = cls.__module__.replace("pyspark", "org.apache.spark")
        return ".".join([java_package, cls.__name__])

    @classmethod
    def _load_java_obj(cls):
        """
        Load the peer Java object.
        """
        java_class = cls._java_loader_class()
        java_obj = _jvm()
        for name in java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj

    @classmethod
    @since("2.0.0")
    def read(cls):
        """Returns an MLReader instance for this class."""
        instance = cls()
        instance._java_obj = cls._load_java_obj()
        return MLReader(instance)

    @classmethod
    @since("2.0.0")
    def load(cls, path):
        """Reads an ML instance from the input path, a shortcut of `read().load(path)`."""
        return cls.read().load(path)

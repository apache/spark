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
import array
import sys
if sys.version > '3':
    basestring = str
    xrange = range
    unicode = str

from abc import ABCMeta
import copy
import numpy as np

from py4j.java_gateway import JavaObject

from pyspark.ml.linalg import DenseVector, Vector, Matrix
from pyspark.ml.util import Identifiable


__all__ = ['Param', 'Params', 'TypeConverters']


class Param(object):
    """
    A param with self-contained documentation.

    .. versionadded:: 1.3.0
    """

    def __init__(self, parent, name, doc, typeConverter=None):
        if not isinstance(parent, Identifiable):
            raise TypeError("Parent must be an Identifiable but got type %s." % type(parent))
        self.parent = parent.uid
        self.name = str(name)
        self.doc = str(doc)
        self.typeConverter = TypeConverters.identity if typeConverter is None else typeConverter

    def _copy_new_parent(self, parent):
        """Copy the current param to a new parent, must be a dummy param."""
        if self.parent == "undefined":
            param = copy.copy(self)
            param.parent = parent.uid
            return param
        else:
            raise ValueError("Cannot copy from non-dummy parent %s." % parent)

    def __str__(self):
        return str(self.parent) + "__" + self.name

    def __repr__(self):
        return "Param(parent=%r, name=%r, doc=%r)" % (self.parent, self.name, self.doc)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if isinstance(other, Param):
            return self.parent == other.parent and self.name == other.name
        else:
            return False


class TypeConverters(object):
    """
    .. note:: DeveloperApi

    Factory methods for common type conversion functions for `Param.typeConverter`.

    .. versionadded:: 2.0.0
    """

    @staticmethod
    def _is_numeric(value):
        vtype = type(value)
        return vtype in [int, float, np.float64, np.int64] or vtype.__name__ == 'long'

    @staticmethod
    def _is_integer(value):
        return TypeConverters._is_numeric(value) and float(value).is_integer()

    @staticmethod
    def _can_convert_to_list(value):
        vtype = type(value)
        return vtype in [list, np.ndarray, tuple, xrange, array.array] or isinstance(value, Vector)

    @staticmethod
    def _can_convert_to_string(value):
        vtype = type(value)
        return isinstance(value, basestring) or vtype in [np.unicode_, np.string_, np.str_]

    @staticmethod
    def identity(value):
        """
        Dummy converter that just returns value.
        """
        return value

    @staticmethod
    def toList(value):
        """
        Convert a value to a list, if possible.
        """
        if type(value) == list:
            return value
        elif type(value) in [np.ndarray, tuple, xrange, array.array]:
            return list(value)
        elif isinstance(value, Vector):
            return list(value.toArray())
        else:
            raise TypeError("Could not convert %s to list" % value)

    @staticmethod
    def toListFloat(value):
        """
        Convert a value to list of floats, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._is_numeric(v), value)):
                return [float(v) for v in value]
        raise TypeError("Could not convert %s to list of floats" % value)

    @staticmethod
    def toListInt(value):
        """
        Convert a value to list of ints, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._is_integer(v), value)):
                return [int(v) for v in value]
        raise TypeError("Could not convert %s to list of ints" % value)

    @staticmethod
    def toListString(value):
        """
        Convert a value to list of strings, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._can_convert_to_string(v), value)):
                return [TypeConverters.toString(v) for v in value]
        raise TypeError("Could not convert %s to list of strings" % value)

    @staticmethod
    def toVector(value):
        """
        Convert a value to a MLlib Vector, if possible.
        """
        if isinstance(value, Vector):
            return value
        elif TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._is_numeric(v), value)):
                return DenseVector(value)
        raise TypeError("Could not convert %s to vector" % value)

    @staticmethod
    def toMatrix(value):
        """
        Convert a value to a MLlib Matrix, if possible.
        """
        if isinstance(value, Matrix):
            return value
        raise TypeError("Could not convert %s to matrix" % value)

    @staticmethod
    def toFloat(value):
        """
        Convert a value to a float, if possible.
        """
        if TypeConverters._is_numeric(value):
            return float(value)
        else:
            raise TypeError("Could not convert %s to float" % value)

    @staticmethod
    def toInt(value):
        """
        Convert a value to an int, if possible.
        """
        if TypeConverters._is_integer(value):
            return int(value)
        else:
            raise TypeError("Could not convert %s to int" % value)

    @staticmethod
    def toString(value):
        """
        Convert a value to a string, if possible.
        """
        if isinstance(value, basestring):
            return value
        elif type(value) in [np.string_, np.str_]:
            return str(value)
        elif type(value) == np.unicode_:
            return unicode(value)
        else:
            raise TypeError("Could not convert %s to string type" % type(value))

    @staticmethod
    def toBoolean(value):
        """
        Convert a value to a boolean, if possible.
        """
        if type(value) == bool:
            return value
        else:
            raise TypeError("Boolean Param requires value of type bool. Found %s." % type(value))


class Params(Identifiable):
    """
    Components that take parameters. This also provides an internal
    param map to store parameter values attached to the instance.

    .. versionadded:: 1.3.0
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(Params, self).__init__()
        #: internal param map for user-supplied values param map
        self._paramMap = {}

        #: internal param map for default values
        self._defaultParamMap = {}

        #: value returned by :py:func:`params`
        self._params = None

        # Copy the params from the class to the object
        self._copy_params()

    def _copy_params(self):
        """
        Copy all params defined on the class to current object.
        """
        cls = type(self)
        src_name_attrs = [(x, getattr(cls, x)) for x in dir(cls)]
        src_params = list(filter(lambda nameAttr: isinstance(nameAttr[1], Param), src_name_attrs))
        for name, param in src_params:
            setattr(self, name, param._copy_new_parent(self))

    @property
    def params(self):
        """
        Returns all params ordered by name. The default implementation
        uses :py:func:`dir` to get all attributes of type
        :py:class:`Param`.
        """
        if self._params is None:
            self._params = list(filter(lambda attr: isinstance(attr, Param),
                                       [getattr(self, x) for x in dir(self) if x != "params" and
                                        not isinstance(getattr(type(self), x, None), property)]))
        return self._params

    def explainParam(self, param):
        """
        Explains a single param and returns its name, doc, and optional
        default value and user-supplied value in a string.
        """
        param = self._resolveParam(param)
        values = []
        if self.isDefined(param):
            if param in self._defaultParamMap:
                values.append("default: %s" % self._defaultParamMap[param])
            if param in self._paramMap:
                values.append("current: %s" % self._paramMap[param])
        else:
            values.append("undefined")
        valueStr = "(" + ", ".join(values) + ")"
        return "%s: %s %s" % (param.name, param.doc, valueStr)

    def explainParams(self):
        """
        Returns the documentation of all params with their optionally
        default values and user-supplied values.
        """
        return "\n".join([self.explainParam(param) for param in self.params])

    def getParam(self, paramName):
        """
        Gets a param by its name.
        """
        param = getattr(self, paramName)
        if isinstance(param, Param):
            return param
        else:
            raise ValueError("Cannot find param with name %s." % paramName)

    def isSet(self, param):
        """
        Checks whether a param is explicitly set by user.
        """
        param = self._resolveParam(param)
        return param in self._paramMap

    def hasDefault(self, param):
        """
        Checks whether a param has a default value.
        """
        param = self._resolveParam(param)
        return param in self._defaultParamMap

    def isDefined(self, param):
        """
        Checks whether a param is explicitly set by user or has
        a default value.
        """
        return self.isSet(param) or self.hasDefault(param)

    def hasParam(self, paramName):
        """
        Tests whether this instance contains a param with a given
        (string) name.
        """
        if isinstance(paramName, basestring):
            p = getattr(self, paramName, None)
            return isinstance(p, Param)
        else:
            raise TypeError("hasParam(): paramName must be a string")

    def getOrDefault(self, param):
        """
        Gets the value of a param in the user-supplied param map or its
        default value. Raises an error if neither is set.
        """
        param = self._resolveParam(param)
        if param in self._paramMap:
            return self._paramMap[param]
        else:
            return self._defaultParamMap[param]

    def extractParamMap(self, extra=None):
        """
        Extracts the embedded default param values and user-supplied
        values, and then merges them with extra values from input into
        a flat param map, where the latter value is used if there exist
        conflicts, i.e., with ordering: default param values <
        user-supplied values < extra.

        :param extra: extra param values
        :return: merged param map
        """
        if extra is None:
            extra = dict()
        paramMap = self._defaultParamMap.copy()
        paramMap.update(self._paramMap)
        paramMap.update(extra)
        return paramMap

    def copy(self, extra=None):
        """
        Creates a copy of this instance with the same uid and some
        extra params. The default implementation creates a
        shallow copy using :py:func:`copy.copy`, and then copies the
        embedded and extra parameters over and returns the copy.
        Subclasses should override this method if the default approach
        is not sufficient.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        that = copy.copy(self)
        that._paramMap = {}
        that._defaultParamMap = {}
        return self._copyValues(that, extra)

    def set(self, param, value):
        """
        Sets a parameter in the embedded param map.
        """
        self._shouldOwn(param)
        try:
            value = param.typeConverter(value)
        except ValueError as e:
            raise ValueError('Invalid param value given for param "%s". %s' % (param.name, e))
        self._paramMap[param] = value

    def _shouldOwn(self, param):
        """
        Validates that the input param belongs to this Params instance.
        """
        if not (self.uid == param.parent and self.hasParam(param.name)):
            raise ValueError("Param %r does not belong to %r." % (param, self))

    def _resolveParam(self, param):
        """
        Resolves a param and validates the ownership.

        :param param: param name or the param instance, which must
                      belong to this Params instance
        :return: resolved param instance
        """
        if isinstance(param, Param):
            self._shouldOwn(param)
            return param
        elif isinstance(param, basestring):
            return self.getParam(param)
        else:
            raise ValueError("Cannot resolve %r as a param." % param)

    @staticmethod
    def _dummy():
        """
        Returns a dummy Params instance used as a placeholder to
        generate docs.
        """
        dummy = Params()
        dummy.uid = "undefined"
        return dummy

    def _set(self, **kwargs):
        """
        Sets user-supplied params.
        """
        for param, value in kwargs.items():
            p = getattr(self, param)
            if value is not None:
                try:
                    value = p.typeConverter(value)
                except TypeError as e:
                    raise TypeError('Invalid param value given for param "%s". %s' % (p.name, e))
            self._paramMap[p] = value
        return self

    def _clear(self, param):
        """
        Clears a param from the param map if it has been explicitly set.
        """
        if self.isSet(param):
            del self._paramMap[param]

    def _setDefault(self, **kwargs):
        """
        Sets default params.
        """
        for param, value in kwargs.items():
            p = getattr(self, param)
            if value is not None and not isinstance(value, JavaObject):
                try:
                    value = p.typeConverter(value)
                except TypeError as e:
                    raise TypeError('Invalid default param value given for param "%s". %s'
                                    % (p.name, e))
            self._defaultParamMap[p] = value
        return self

    def _copyValues(self, to, extra=None):
        """
        Copies param values from this instance to another instance for
        params shared by them.

        :param to: the target instance
        :param extra: extra params to be copied
        :return: the target instance with param values copied
        """
        paramMap = self._paramMap.copy()
        if extra is not None:
            paramMap.update(extra)
        for param in self.params:
            # copy default params
            if param in self._defaultParamMap and to.hasParam(param.name):
                to._defaultParamMap[to.getParam(param.name)] = self._defaultParamMap[param]
            # copy explicitly set params
            if param in paramMap and to.hasParam(param.name):
                to._set(**{param.name: paramMap[param]})
        return to

    def _resetUid(self, newUid):
        """
        Changes the uid of this instance. This updates both
        the stored uid and the parent uid of params and param maps.
        This is used by persistence (loading).
        :param newUid: new uid to use, which is converted to unicode
        :return: same instance, but with the uid and Param.parent values
                 updated, including within param maps
        """
        newUid = unicode(newUid)
        self.uid = newUid
        newDefaultParamMap = dict()
        newParamMap = dict()
        for param in self.params:
            newParam = copy.copy(param)
            newParam.parent = newUid
            if param in self._defaultParamMap:
                newDefaultParamMap[newParam] = self._defaultParamMap[param]
            if param in self._paramMap:
                newParamMap[newParam] = self._paramMap[param]
            param.parent = newUid
        self._defaultParamMap = newDefaultParamMap
        self._paramMap = newParamMap
        return self

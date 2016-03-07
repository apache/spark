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

from abc import ABCMeta
import copy

from pyspark import since
from pyspark.ml.util import Identifiable


__all__ = ['Param', 'Params']


class Param(object):
    """
    A param with self-contained documentation.

    .. versionadded:: 1.3.0
    """

    def __init__(self, parent, name, doc, expectedType=None):
        if not isinstance(parent, Identifiable):
            raise TypeError("Parent must be an Identifiable but got type %s." % type(parent))
        self.parent = parent.uid
        self.name = str(name)
        self.doc = str(doc)
        self.expectedType = expectedType

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
    @since("1.3.0")
    def params(self):
        """
        Returns all params ordered by name. The default implementation
        uses :py:func:`dir` to get all attributes of type
        :py:class:`Param`.
        """
        if self._params is None:
            self._params = list(filter(lambda attr: isinstance(attr, Param),
                                       [getattr(self, x) for x in dir(self) if x != "params"]))
        return self._params

    @since("1.4.0")
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

    @since("1.4.0")
    def explainParams(self):
        """
        Returns the documentation of all params with their optionally
        default values and user-supplied values.
        """
        return "\n".join([self.explainParam(param) for param in self.params])

    @since("1.4.0")
    def getParam(self, paramName):
        """
        Gets a param by its name.
        """
        param = getattr(self, paramName)
        if isinstance(param, Param):
            return param
        else:
            raise ValueError("Cannot find param with name %s." % paramName)

    @since("1.4.0")
    def isSet(self, param):
        """
        Checks whether a param is explicitly set by user.
        """
        param = self._resolveParam(param)
        return param in self._paramMap

    @since("1.4.0")
    def hasDefault(self, param):
        """
        Checks whether a param has a default value.
        """
        param = self._resolveParam(param)
        return param in self._defaultParamMap

    @since("1.4.0")
    def isDefined(self, param):
        """
        Checks whether a param is explicitly set by user or has
        a default value.
        """
        return self.isSet(param) or self.hasDefault(param)

    @since("1.4.0")
    def hasParam(self, paramName):
        """
        Tests whether this instance contains a param with a given
        (string) name.
        """
        if isinstance(paramName, str):
            p = getattr(self, paramName, None)
            return isinstance(p, Param)
        else:
            raise TypeError("hasParam(): paramName must be a string")

    @since("1.4.0")
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

    @since("1.4.0")
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

    @since("1.4.0")
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
        that._paramMap = self.extractParamMap(extra)
        return that

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
        elif isinstance(param, str):
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
            if p.expectedType is None or type(value) == p.expectedType or value is None:
                self._paramMap[getattr(self, param)] = value
            else:
                try:
                    # Try and do "safe" conversions that don't lose information
                    if p.expectedType == float:
                        self._paramMap[getattr(self, param)] = float(value)
                    # Python 3 unified long & int
                    elif p.expectedType == int and type(value).__name__ == 'long':
                        self._paramMap[getattr(self, param)] = value
                    else:
                        raise Exception(
                            "Provided type {0} incompatible with type {1} for param {2}"
                            .format(type(value), p.expectedType, p))
                except ValueError:
                    raise Exception(("Failed to convert {0} to type {1} for param {2}"
                                     .format(type(value), p.expectedType, p)))
        return self

    def _setDefault(self, **kwargs):
        """
        Sets default params.
        """
        for param, value in kwargs.items():
            self._defaultParamMap[getattr(self, param)] = value
        return self

    def _copyValues(self, to, extra=None):
        """
        Copies param values from this instance to another instance for
        params shared by them.

        :param to: the target instance
        :param extra: extra params to be copied
        :return: the target instance with param values copied
        """
        if extra is None:
            extra = dict()
        paramMap = self.extractParamMap(extra)
        for p in self.params:
            if p in paramMap and to.hasParam(p.name):
                to._set(**{p.name: paramMap[p]})
        return to

    def _resetUid(self, newUid):
        """
        Changes the uid of this instance. This updates both
        the stored uid and the parent uid of params and param maps.
        This is used by persistence (loading).
        :param newUid: new uid to use
        :return: same instance, but with the uid and Param.parent values
                 updated, including within param maps
        """
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

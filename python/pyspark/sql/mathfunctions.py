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

"""
A collection of builtin math functions
"""

from pyspark import SparkContext
from pyspark.sql.dataframe import Column

__all__ = []


def _create_unary_mathfunction(name, doc=""):
    """ Create a unary mathfunction by name"""
    def _(col):
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.mathfunctions, name)(col._jc if isinstance(col, Column) else col)
        return Column(jc)
    _.__name__ = name
    _.__doc__ = doc
    return _


def _create_binary_mathfunction(name, doc=""):
    """ Create a binary mathfunction by name"""
    def _(col1, col2):
        sc = SparkContext._active_spark_context
        # users might write ints for simplicity. This would throw an error on the JVM side.
        if type(col1) is int:
            col1 = col1 * 1.0
        if type(col2) is int:
            col2 = col2 * 1.0
        jc = getattr(sc._jvm.mathfunctions, name)(col1._jc if isinstance(col1, Column) else col1,
                                                  col2._jc if isinstance(col2, Column) else col2)
        return Column(jc)
    _.__name__ = name
    _.__doc__ = doc
    return _


# math functions are found under another object therefore, they need to be handled separately
_mathfunctions = {
    'acos': 'Computes the cosine inverse of the given value; the returned angle is in the range' +
            '0.0 through pi.',
    'asin': 'Computes the sine inverse of the given value; the returned angle is in the range' +
            '-pi/2 through pi/2.',
    'atan': 'Computes the tangent inverse of the given value.',
    'cbrt': 'Computes the cube-root of the given value.',
    'ceil': 'Computes the ceiling of the given value.',
    'cos': 'Computes the cosine of the given value.',
    'cosh': 'Computes the hyperbolic cosine of the given value.',
    'exp': 'Computes the exponential of the given value.',
    'expm1': 'Computes the exponential of the given value minus one.',
    'floor': 'Computes the floor of the given value.',
    'log': 'Computes the natural logarithm of the given value.',
    'log10': 'Computes the logarithm of the given value in Base 10.',
    'log1p': 'Computes the natural logarithm of the given value plus one.',
    'rint': 'Returns the double value that is closest in value to the argument and' +
            ' is equal to a mathematical integer.',
    'signum': 'Computes the signum of the given value.',
    'sin': 'Computes the sine of the given value.',
    'sinh': 'Computes the hyperbolic sine of the given value.',
    'tan': 'Computes the tangent of the given value.',
    'tanh': 'Computes the hyperbolic tangent of the given value.',
    'toDeg': 'Converts an angle measured in radians to an approximately equivalent angle ' +
             'measured in degrees.',
    'toRad': 'Converts an angle measured in degrees to an approximately equivalent angle ' +
             'measured in radians.'
}

# math functions that take two arguments as input
_binary_mathfunctions = {
    'atan2': 'Returns the angle theta from the conversion of rectangular coordinates (x, y) to' +
             'polar coordinates (r, theta).',
    'hypot': 'Computes `sqrt(a^2^ + b^2^)` without intermediate overflow or underflow.',
    'pow': 'Returns the value of the first argument raised to the power of the second argument.'
}

for _name, _doc in _mathfunctions.items():
    globals()[_name] = _create_unary_mathfunction(_name, _doc)
for _name, _doc in _binary_mathfunctions.items():
    globals()[_name] = _create_binary_mathfunction(_name, _doc)
del _name, _doc
__all__ += _mathfunctions.keys()
__all__ += _binary_mathfunctions.keys()
__all__.sort()

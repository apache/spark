#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Module defining global singleton classes.

This module raises a RuntimeError if an attempt to reload it is made. In that
way the identities of the classes defined here are fixed and will remain so
even if pyspark itself is reloaded. In particular, a function like the following
will still work correctly after pyspark is reloaded:

    def foo(arg=pyspark._NoValue):
        if arg is pyspark._NoValue:
            ...

See gh-7844 for a discussion of the reload problem that motivated this module.

Note that this approach is taken after from NumPy.
"""

__ALL__ = ["_NoValue"]


# Disallow reloading this module so as to preserve the identities of the
# classes defined here.
if "_is_loaded" in globals():
    raise RuntimeError("Reloading pyspark._globals is not allowed")
_is_loaded = True


class _NoValueType(object):
    """Special keyword value.

    The instance of this class may be used as the default value assigned to a
    deprecated keyword in order to check if it has been given a user defined
    value.

    This class was copied from NumPy.
    """

    __instance = None

    def __new__(cls):
        # ensure that only one instance exists
        if not cls.__instance:
            cls.__instance = super(_NoValueType, cls).__new__(cls)
        return cls.__instance

    # needed for python 2 to preserve identity through a pickle
    def __reduce__(self):
        return (self.__class__, ())

    def __repr__(self):
        return "<no value>"


_NoValue = _NoValueType()

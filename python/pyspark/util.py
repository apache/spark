# -*- coding: utf-8 -*-
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

import re
import sys
import inspect
from py4j.protocol import Py4JJavaError

__all__ = []


def _exception_message(excp):
    """Return the message from an exception as either a str or unicode object.  Supports both
    Python 2 and Python 3.

    >>> msg = "Exception message"
    >>> excp = Exception(msg)
    >>> msg == _exception_message(excp)
    True

    >>> msg = u"unicöde"
    >>> excp = Exception(msg)
    >>> msg == _exception_message(excp)
    True
    """
    if isinstance(excp, Py4JJavaError):
        # 'Py4JJavaError' doesn't contain the stack trace available on the Java side in 'message'
        # attribute in Python 2. We should call 'str' function on this exception in general but
        # 'Py4JJavaError' has an issue about addressing non-ascii strings. So, here we work
        # around by the direct call, '__str__()'. Please see SPARK-23517.
        return excp.__str__()
    if hasattr(excp, "message"):
        return excp.message
    return str(excp)


def _get_argspec(f):
    """
    Get argspec of a function. Supports both Python 2 and Python 3.
    """
    if sys.version_info[0] < 3:
        argspec = inspect.getargspec(f)
    else:
        # `getargspec` is deprecated since python3.0 (incompatible with function annotations).
        # See SPARK-23569.
        argspec = inspect.getfullargspec(f)
    return argspec


class VersionUtils(object):
    """
    Provides utility method to determine Spark versions with given input string.
    """
    @staticmethod
    def majorMinorVersion(sparkVersion):
        """
        Given a Spark version string, return the (major version number, minor version number).
        E.g., for 2.0.1-SNAPSHOT, return (2, 0).

        >>> sparkVersion = "2.4.0"
        >>> VersionUtils.majorMinorVersion(sparkVersion)
        (2, 4)
        >>> sparkVersion = "2.3.0-SNAPSHOT"
        >>> VersionUtils.majorMinorVersion(sparkVersion)
        (2, 3)

        """
        m = re.search(r'^(\d+)\.(\d+)(\..*)?$', sparkVersion)
        if m is not None:
            return (int(m.group(1)), int(m.group(2)))
        else:
            raise ValueError("Spark tried to parse '%s' as a Spark" % sparkVersion +
                             " version string, but it could not find the major and minor" +
                             " version numbers.")


def fail_on_stopiteration(f):
    """
    Wraps the input function to fail on 'StopIteration' by raising a 'RuntimeError'
    prevents silent loss of data when 'f' is used in a for loop in Spark code
    """
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except StopIteration as exc:
            raise RuntimeError(
                "Caught StopIteration thrown from user's code; failing the task",
                exc
            )

    return wrapper


if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)

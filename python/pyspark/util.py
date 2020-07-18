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
import traceback

__all__ = []


def print_exec(stream):
    ei = sys.exc_info()
    traceback.print_exception(ei[0], ei[1], ei[2], None, stream)


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


def _print_missing_jar(lib_name, pkg_name, jar_name, spark_version):
    print("""
________________________________________________________________________________________________

  Spark %(lib_name)s libraries not found in class path. Try one of the following.

  1. Include the %(lib_name)s library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-%(pkg_name)s:%(spark_version)s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-%(jar_name)s, Version = %(spark_version)s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-%(jar_name)s.jar> ...

________________________________________________________________________________________________

""" % {
        "lib_name": lib_name,
        "pkg_name": pkg_name,
        "jar_name": jar_name,
        "spark_version": spark_version
    })


def _parse_memory(s):
    """
    Parse a memory string in the format supported by Java (e.g. 1g, 200m) and
    return the value in MiB

    >>> _parse_memory("256m")
    256
    >>> _parse_memory("2g")
    2048
    """
    units = {'g': 1024, 'm': 1, 't': 1 << 20, 'k': 1.0 / 1024}
    if s[-1].lower() not in units:
        raise ValueError("invalid format: " + s)
    return int(float(s[:-1]) * units[s[-1].lower()])

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)

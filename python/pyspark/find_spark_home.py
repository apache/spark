#!/usr/bin/env python

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

# This script attempt to determine the correct setting for SPARK_HOME given
# that Spark may have been installed on the system with pip.

from __future__ import print_function
import os
import sys


def _find_spark_home():
    """Find the SPARK_HOME."""
    # If the environment has SPARK_HOME set trust it.
    if "SPARK_HOME" in os.environ:
        return os.environ["SPARK_HOME"]

    def is_spark_home(path):
        """Takes a path and returns true if the provided path could be a reasonable SPARK_HOME"""
        return (os.path.isfile(os.path.join(path, "bin/spark-submit")) and
                (os.path.isdir(os.path.join(path, "jars")) or
                 os.path.isdir(os.path.join(path, "assembly"))))

    paths = ["../", os.path.dirname(os.path.realpath(__file__))]

    # Add the path of the PySpark module if it exists
    import_error_raised = False
    if sys.version < "3":
        import imp
        try:
            module_home = imp.find_module("pyspark")[1]
            paths.append(module_home)
            # If we are installed in edit mode also look two dirs up
            paths.append(os.path.join(module_home, "../../"))
        except ImportError:
            # Not pip installed no worries
            import_error_raised = True
    else:
        from importlib.util import find_spec
        try:
            module_home = os.path.dirname(find_spec("pyspark").origin)
            paths.append(module_home)
            # If we are installed in edit mode also look two dirs up
            paths.append(os.path.join(module_home, "../../"))
        except ImportError:
            # Not pip installed no worries
            import_error_raised = True

    # Normalize the paths
    paths = [os.path.abspath(p) for p in paths]

    try:
        return next(path for path in paths if is_spark_home(path))
    except StopIteration:
        print("Could not find valid SPARK_HOME while searching {0}".format(paths), file=sys.stderr)
        if import_error_raised:
            print(
                "\nDid you install PySpark via a package manager such as pip or Conda? If so,\n"
                "PySpark was not found in your Python environment. It is possible your\n"
                "Python environment does not properly bind with your package manager.\n"
                "\nPlease check your default 'python' and if you set PYSPARK_PYTHON and/or\n"
                "PYSPARK_DRIVER_PYTHON environment variables, and see if you can import\n"
                "PySpark, for example, 'python -c 'import pyspark'.\n"
                "\nIf you cannot import, you can install by using the Python executable directly,\n"
                "for example, 'python -m pip install pyspark [--user]'. Otherwise, you can also\n"
                "explicitly set the Python executable, that has PySpark installed, to\n"
                "PYSPARK_PYTHON or PYSPARK_DRIVER_PYTHON environment variables, for example,\n"
                "'PYSPARK_PYTHON=python3 pyspark'.\n", file=sys.stderr)
        sys.exit(-1)

if __name__ == "__main__":
    print(_find_spark_home())

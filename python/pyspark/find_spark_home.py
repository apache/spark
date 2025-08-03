#!/usr/bin/env python3

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

import os
import sys
import argparse


def _validate_spark_home(spark_home):
    """Validate that the found SPARK_HOME is actually a valid Spark installation.
    
    Parameters
    ----------
    spark_home : str
        Path to the potential SPARK_HOME directory
        
    Returns
    -------
    bool
        True if the path is a valid Spark installation, False otherwise
    """
    if not spark_home or not os.path.isdir(spark_home):
        return False
    
    # Check for essential Spark files and directories
    required_files = [
        "bin/spark-submit",
        "README.md",
        "LICENSE"
    ]
    
    required_dirs = [
        "bin",
        "conf"
    ]
    
    # At least one of these should exist
    jar_dirs = [
        "jars",
        "assembly"
    ]
    
    # Check required files
    for file_path in required_files:
        if not os.path.isfile(os.path.join(spark_home, file_path)):
            return False
    
    # Check required directories
    for dir_path in required_dirs:
        if not os.path.isdir(os.path.join(spark_home, dir_path)):
            return False
    
    # Check that at least one jar directory exists
    jar_dir_exists = any(
        os.path.isdir(os.path.join(spark_home, jar_dir)) 
        for jar_dir in jar_dirs
    )
    
    return jar_dir_exists


def _find_spark_home(debug=False):
    """Find the SPARK_HOME with enhanced validation and debugging.
    
    Parameters
    ----------
    debug : bool, optional
        If True, print debug information about the search process
        
    Returns
    -------
    str
        The validated SPARK_HOME path
    """
    # If the environment has SPARK_HOME set trust it.
    if "SPARK_HOME" in os.environ:
        spark_home = os.environ["SPARK_HOME"]
        if debug:
            print(f"Found SPARK_HOME in environment: {spark_home}", file=sys.stderr)
        
        if _validate_spark_home(spark_home):
            return spark_home
        else:
            if debug:
                print(f"Environment SPARK_HOME is invalid: {spark_home}", file=sys.stderr)
    
    def is_spark_home(path):
        """Takes a path and returns true if the provided path could be a reasonable SPARK_HOME"""
        return os.path.isfile(os.path.join(path, "bin/spark-submit")) and (
            os.path.isdir(os.path.join(path, "jars"))
            or os.path.isdir(os.path.join(path, "assembly"))
        )

    # Spark distribution can be downloaded when PYSPARK_HADOOP_VERSION environment variable is set.
    # We should look up this directory first, see also SPARK-32017.
    spark_dist_dir = "spark-distribution"
    paths = [
        "../",  # When we're in spark/python.
    ]

    if "__file__" in globals():
        paths += [
            # Two case belows are valid when the current script is called as a library.
            os.path.join(os.path.dirname(os.path.realpath(__file__)), spark_dist_dir),
            os.path.dirname(os.path.realpath(__file__)),
        ]

    # Add the path of the PySpark module if it exists
    import_error_raised = False
    from importlib.util import find_spec

    try:
        module_home = os.path.dirname(find_spec("pyspark").origin)
        paths.append(os.path.join(module_home, spark_dist_dir))
        paths.append(module_home)
        # If we are installed in edit mode also look two dirs up
        # Downloading different versions are not supported in edit mode.
        paths.append(os.path.join(module_home, "../../"))
    except ImportError:
        # Not pip installed no worries
        import_error_raised = True

    # Normalize the paths
    paths = [os.path.abspath(p) for p in paths]
    
    if debug:
        print(f"Searching for Spark installation in paths: {paths}", file=sys.stderr)

    try:
        spark_home = next(path for path in paths if is_spark_home(path))
        
        # Additional validation
        if _validate_spark_home(spark_home):
            if debug:
                print(f"Found valid SPARK_HOME: {spark_home}", file=sys.stderr)
            return spark_home
        else:
            if debug:
                print(f"Found SPARK_HOME but validation failed: {spark_home}", file=sys.stderr)
            raise StopIteration
            
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
                "'PYSPARK_PYTHON=python3 pyspark'.\n",
                file=sys.stderr,
            )
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find SPARK_HOME directory")
    parser.add_argument("--debug", action="store_true", 
                       help="Enable debug output")
    args = parser.parse_args()
    
    print(_find_spark_home(debug=args.debug))

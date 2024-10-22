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
An interactive shell.

This file is designed to be launched as a PYTHONSTARTUP script.
"""

import atexit
import builtins
import os
import platform
import warnings
import sys

import pyspark
from pyspark.core.context import SparkContext
from pyspark.logger import SPARK_LOG_SCHEMA  # noqa: F401
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.utils import is_remote
from urllib.parse import urlparse

if getattr(builtins, "__IPYTHON__", False):
    # (Only) during PYTHONSTARTUP execution, IPython temporarily adds the parent
    # directory of the script into the Python path, which results in searching
    # packages under `pyspark` directory.
    # For example, `import pandas` attempts to import `pyspark.pandas`, see also SPARK-42266.
    if "__file__" in globals():
        parent_dir = os.path.abspath(os.path.dirname(__file__))
        if parent_dir in sys.path:
            sys.path.remove(parent_dir)

if is_remote():
    try:
        # Creates pyspark.sql.connect.SparkSession.
        spark = SparkSession.builder.getOrCreate()

        from pyspark.sql.connect.shell import PROGRESS_BAR_ENABLED

        # Check if th eprogress bar needs to be disabled.
        if PROGRESS_BAR_ENABLED not in os.environ:
            os.environ[PROGRESS_BAR_ENABLED] = "1"
        else:
            val = os.getenv(PROGRESS_BAR_ENABLED, "false")
            if val.lower().strip() == "false":
                os.environ[PROGRESS_BAR_ENABLED] = "0"
            elif val.lower().strip() == "true":
                os.environ[PROGRESS_BAR_ENABLED] = "1"

        val = os.environ[PROGRESS_BAR_ENABLED]
        if val not in ("1", "0"):
            raise ValueError(
                f"Environment variable '{PROGRESS_BAR_ENABLED}' must "
                f"be set to either 1 or 0, found: {val}"
            )

    except Exception:
        import sys
        import traceback

        warnings.warn("Failed to initialize Spark session.")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    version = pyspark.__version__
    sc = None
else:
    if os.environ.get("SPARK_EXECUTOR_URI"):
        SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

    SparkContext._ensure_initialized()

    try:
        spark = SparkSession._create_shell_session()
    except Exception:
        import sys
        import traceback

        warnings.warn("Failed to initialize Spark session.")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    sc = spark.sparkContext
    atexit.register((lambda sc: lambda: sc.stop())(sc))

    # for compatibility
    sqlContext = SQLContext._get_or_create(sc)
    sqlCtx = sqlContext
    version = sc.version

sql = spark.sql

print(
    r"""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version %s
      /_/
"""
    % version
)
print(
    "Using Python version %s (%s, %s)"
    % (platform.python_version(), platform.python_build()[0], platform.python_build()[1])
)
if is_remote():
    url = os.environ.get("SPARK_REMOTE", None)
    assert url is not None
    if url.startswith("local"):
        url = "sc://localhost"  # only for display in the console.
    print("Client connected to the Spark Connect server at %s" % urlparse(url).netloc)
else:
    print("Spark context Web UI available at %s" % (sc.uiWebUrl))  # type: ignore[union-attr]
    print(
        "Spark context available as 'sc' (master = %s, app id = %s)."
        % (sc.master, sc.applicationId)  # type: ignore[union-attr]
    )

print("SparkSession available as 'spark'.")

# The ./bin/pyspark script stores the old PYTHONSTARTUP value in OLD_PYTHONSTARTUP,
# which allows us to execute the user's PYTHONSTARTUP file:
_pythonstartup = os.environ.get("OLD_PYTHONSTARTUP")
if _pythonstartup and os.path.isfile(_pythonstartup):
    with open(_pythonstartup) as f:
        code = compile(f.read(), _pythonstartup, "exec")
        exec(code)

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


from typing import Any, Callable


def require_minimum_pandas_version() -> None:
    """Raise ImportError if minimum version of Pandas is not installed"""
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pandas_version = "1.0.5"

    from distutils.version import LooseVersion

    try:
        import pandas

        have_pandas = True
    except ImportError as error:
        have_pandas = False
        raised_error = error
    if not have_pandas:
        raise ImportError(
            "Pandas >= %s must be installed; however, " "it was not found." % minimum_pandas_version
        ) from raised_error
    if LooseVersion(pandas.__version__) < LooseVersion(minimum_pandas_version):
        raise ImportError(
            "Pandas >= %s must be installed; however, "
            "your version was %s." % (minimum_pandas_version, pandas.__version__)
        )


def require_minimum_pyarrow_version() -> None:
    """Raise ImportError if minimum version of pyarrow is not installed"""
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pyarrow_version = "1.0.0"

    from distutils.version import LooseVersion
    import os

    try:
        import pyarrow

        have_arrow = True
    except ImportError as error:
        have_arrow = False
        raised_error = error
    if not have_arrow:
        raise ImportError(
            "PyArrow >= %s must be installed; however, "
            "it was not found." % minimum_pyarrow_version
        ) from raised_error
    if LooseVersion(pyarrow.__version__) < LooseVersion(minimum_pyarrow_version):
        raise ImportError(
            "PyArrow >= %s must be installed; however, "
            "your version was %s." % (minimum_pyarrow_version, pyarrow.__version__)
        )
    if os.environ.get("ARROW_PRE_0_15_IPC_FORMAT", "0") == "1":
        raise RuntimeError(
            "Arrow legacy IPC format is not supported in PySpark, "
            "please unset ARROW_PRE_0_15_IPC_FORMAT"
        )


def pyarrow_version_less_than_minimum(minimum_pyarrow_version: str) -> bool:
    """Return False if the installed pyarrow version is less than minimum_pyarrow_version
    or if pyarrow is not installed."""
    from distutils.version import LooseVersion

    try:
        import pyarrow
    except ImportError:
        return False

    return LooseVersion(pyarrow.__version__) < LooseVersion(minimum_pyarrow_version)


def barrier(f: Callable) -> Callable:
    """Mark functon should be executed in barrier mode. This API is a developer API.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    f : function, :meth:`pyspark.sql.functions.udf` or :meth:`pyspark.sql.functions.pandas_udf`
        a Python function, or a user-defined function. The user-defined function can
        be either row-at-a-time or vectorized. See :meth:`pyspark.sql.functions.udf` and
        :meth:`pyspark.sql.functions.pandas_udf`.

    Returns
    -------
    function
        a user-defined function

    Notes
    -----
    The barrier function is dedicated for external ML frameworks, including PyTorch and XGBoost.
    It is supposed only to be used in mapInPandas and mapInArrow, and followed by a collection to
    get the model coefficients. Also note that some DataFrame operations (e.g. df.show, df.take)
    are not allowed in barrier mode, due to the underlying RDD operations are not supported.

    Examples
    --------
    >>> from pyspark.sql.pandas.utils import barrier

    create a dataframe
    >>> df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

    define a barrier python udf
    >>> @barrier
    ... def filter_func(iterator):
    ...     for pdf in iterator:
    ...         yield pdf[pdf.id == 1]
    ...
    >>> df.mapInPandas(filter_func, df.schema).collect()
    [Row(id=1, age=21)]
    """

    f._is_barrier = True  # type: ignore[attr-defined]
    return f


def _is_barrier(obj: Any) -> bool:
    return (
        obj is not None
        and hasattr(obj, "_is_barrier")
        and isinstance(obj._is_barrier, bool)
        and obj._is_barrier
    )

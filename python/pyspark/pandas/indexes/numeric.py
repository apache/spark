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
from typing import Any, Optional, Union, cast

import pandas as pd
from pandas.api.types import is_hashable

from pyspark import pandas as ps
from pyspark.pandas._typing import Dtype, Name
from pyspark.pandas.indexes.base import Index
from pyspark.pandas.series import Series


class NumericIndex(Index):
    """
    Provide numeric type operations.
    This is an abstract class.
    """

    pass


class IntegerIndex(NumericIndex):
    """
    This is an abstract class for Int64Index.
    """

    pass


class Int64Index(IntegerIndex):
    """
    Immutable sequence used for indexing and alignment. The basic object
    storing axis labels for all pandas objects. Int64Index is a special case
    of `Index` with purely integer labels.

    Parameters
    ----------
    data : array-like (1-dimensional)
    dtype : NumPy dtype (default: int64)
    copy : bool
        Make a copy of input ndarray.
    name : object
        Name to be stored in the index.

    See Also
    --------
    Index : The base pandas-on-Spark Index type.
    Float64Index : A special case of :class:`Index` with purely float labels.

    Notes
    -----
    An Index instance can **only** contain hashable objects.

    Examples
    --------
    >>> ps.Int64Index([1, 2, 3])
    Int64Index([1, 2, 3], dtype='int64')

    From a Series:

    >>> s = ps.Series([1, 2, 3], index=[10, 20, 30])
    >>> ps.Int64Index(s)
    Int64Index([1, 2, 3], dtype='int64')

    From an Index:

    >>> idx = ps.Index([1, 2, 3])
    >>> ps.Int64Index(idx)
    Int64Index([1, 2, 3], dtype='int64')
    """

    def __new__(
        cls,
        data: Optional[Any] = None,
        dtype: Optional[Union[str, Dtype]] = None,
        copy: bool = False,
        name: Optional[Name] = None,
    ) -> "Int64Index":
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, (Series, Index)):
            if dtype is None:
                dtype = "int64"
            return cast(Int64Index, Index(data, dtype=dtype, copy=copy, name=name))

        return cast(
            Int64Index, ps.from_pandas(pd.Int64Index(data=data, dtype=dtype, copy=copy, name=name))
        )


class Float64Index(NumericIndex):
    """
    Immutable sequence used for indexing and alignment. The basic object
    storing axis labels for all pandas objects. Float64Index is a special case
    of `Index` with purely float labels.

    Parameters
    ----------
    data : array-like (1-dimensional)
    dtype : NumPy dtype (default: float64)
    copy : bool
        Make a copy of input ndarray.
    name : object
        Name to be stored in the index.

    See Also
    --------
    Index : The base pandas-on-Spark Index type.
    Int64Index : A special case of :class:`Index` with purely integer labels.

    Notes
    -----
    An Index instance can **only** contain hashable objects.

    Examples
    --------
    >>> ps.Float64Index([1.0, 2.0, 3.0])
    Float64Index([1.0, 2.0, 3.0], dtype='float64')

    From a Series:

    >>> s = ps.Series([1, 2, 3], index=[10, 20, 30])
    >>> ps.Float64Index(s)
    Float64Index([1.0, 2.0, 3.0], dtype='float64')

    From an Index:

    >>> idx = ps.Index([1, 2, 3])
    >>> ps.Float64Index(idx)
    Float64Index([1.0, 2.0, 3.0], dtype='float64')
    """

    def __new__(
        cls,
        data: Optional[Any] = None,
        dtype: Optional[Union[str, Dtype]] = None,
        copy: bool = False,
        name: Optional[Name] = None,
    ) -> "Float64Index":
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, (Series, Index)):
            if dtype is None:
                dtype = "float64"
            return cast(Float64Index, Index(data, dtype=dtype, copy=copy, name=name))

        return cast(
            Float64Index,
            ps.from_pandas(pd.Float64Index(data=data, dtype=dtype, copy=copy, name=name)),
        )


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.indexes.numeric

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.indexes.numeric.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("pyspark.pandas.indexes.numeric tests")
        .getOrCreate()
    )
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.indexes.numeric,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()

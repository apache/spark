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
import pandas as pd
from pandas.api.types import is_hashable

from pyspark import pandas as pp
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
    Index : The base Koalas Index type.
    Float64Index : A special case of :class:`Index` with purely float labels.

    Notes
    -----
    An Index instance can **only** contain hashable objects.

    Examples
    --------
    >>> pp.Int64Index([1, 2, 3])
    Int64Index([1, 2, 3], dtype='int64')

    From a Series:

    >>> s = pp.Series([1, 2, 3], index=[10, 20, 30])
    >>> pp.Int64Index(s)
    Int64Index([1, 2, 3], dtype='int64')

    From an Index:

    >>> idx = pp.Index([1, 2, 3])
    >>> pp.Int64Index(idx)
    Int64Index([1, 2, 3], dtype='int64')
    """

    def __new__(cls, data=None, dtype=None, copy=False, name=None):
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, (Series, Index)):
            if dtype is None:
                dtype = "int64"
            return Index(data, dtype=dtype, copy=copy, name=name)

        return pp.from_pandas(pd.Int64Index(data=data, dtype=dtype, copy=copy, name=name))


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
    Index : The base Koalas Index type.
    Int64Index : A special case of :class:`Index` with purely integer labels.

    Notes
    -----
    An Index instance can **only** contain hashable objects.

    Examples
    --------
    >>> pp.Float64Index([1.0, 2.0, 3.0])
    Float64Index([1.0, 2.0, 3.0], dtype='float64')

    From a Series:

    >>> s = pp.Series([1, 2, 3], index=[10, 20, 30])
    >>> pp.Float64Index(s)
    Float64Index([1.0, 2.0, 3.0], dtype='float64')

    From an Index:

    >>> idx = pp.Index([1, 2, 3])
    >>> pp.Float64Index(idx)
    Float64Index([1.0, 2.0, 3.0], dtype='float64')
    """

    def __new__(cls, data=None, dtype=None, copy=False, name=None):
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, (Series, Index)):
            if dtype is None:
                dtype = "float64"
            return Index(data, dtype=dtype, copy=copy, name=name)

        return pp.from_pandas(pd.Float64Index(data=data, dtype=dtype, copy=copy, name=name))

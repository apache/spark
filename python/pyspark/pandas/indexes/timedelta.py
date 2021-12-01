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
from typing import cast, no_type_check, Any
from functools import partial

import pandas as pd
from pandas.api.types import is_hashable

from pyspark import pandas as ps
from pyspark._globals import _NoValue
from pyspark.pandas.indexes.base import Index
from pyspark.pandas.missing.indexes import MissingPandasLikeTimedeltaIndex
from pyspark.pandas.series import Series


class TimedeltaIndex(Index):
    """
    Immutable ndarray-like of timedelta64 data, represented internally as int64, and
    which can be boxed to timedelta objects.

    See Also
    --------
    Index : The base pandas Index type.
    """

    @no_type_check
    def __new__(
        cls,
        data=None,
        unit=None,
        freq=_NoValue,
        closed=None,
        dtype=None,
        copy=False,
        name=None,
    ) -> "TimedeltaIndex":
        if not is_hashable(name):
            raise TypeError("Index.name must be a hashable type")

        if isinstance(data, (Series, Index)):
            if dtype is None:
                dtype = "timedelta64[ns]"
            return cast(TimedeltaIndex, Index(data, dtype=dtype, copy=copy, name=name))

        kwargs = dict(
            data=data,
            unit=unit,
            closed=closed,
            dtype=dtype,
            copy=copy,
            name=name,
        )
        if freq is not _NoValue:
            kwargs["freq"] = freq
        x = pd.TimedeltaIndex(**kwargs)
        ps.from_pandas(x)
        return cast(TimedeltaIndex, ps.from_pandas(pd.TimedeltaIndex(**kwargs)))

    def __getattr__(self, item: str) -> Any:
        if hasattr(MissingPandasLikeTimedeltaIndex, item):
            property_or_func = getattr(MissingPandasLikeTimedeltaIndex, item)
            if isinstance(property_or_func, property):
                return property_or_func.fget(self)
            else:
                return partial(property_or_func, self)

        raise AttributeError("'TimedeltaIndex' object has no attribute '{}'".format(item))

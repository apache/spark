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
import datetime
import decimal
from typing import Any, Tuple, TypeVar, Union, TYPE_CHECKING

import numpy as np
from pandas.api.extensions import ExtensionDtype

if TYPE_CHECKING:
    from pyspark.pandas.base import IndexOpsMixin
    from pyspark.pandas.frame import DataFrame
    from pyspark.pandas.generic import Frame
    from pyspark.pandas.indexes.base import Index
    from pyspark.pandas.series import Series


# TypeVars
T = TypeVar("T")

FrameLike = TypeVar("FrameLike", bound="Frame")
IndexOpsLike = TypeVar("IndexOpsLike", bound="IndexOpsMixin")

# Type aliases
Scalar = Union[
    int, float, bool, str, bytes, decimal.Decimal, datetime.date, datetime.datetime, None
]

# TODO: use the actual type parameters.
Label = Tuple[Any, ...]
Name = Union[Any, Label]

Axis = Union[int, str]
Dtype = Union[np.dtype, ExtensionDtype]

DataFrameOrSeries = Union["DataFrame", "Series"]
SeriesOrIndex = Union["Series", "Index"]

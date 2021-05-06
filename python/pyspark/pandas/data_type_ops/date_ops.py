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
import warnings

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from pyspark.pandas.base import column_op, IndexOpsMixin
from pyspark.pandas.data_type_ops.base import DataTypeOps


class DateOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: DateType.
    """

    def __add__(self, left, right):
        raise TypeError("addition can not be applied to date.")

    def __sub__(self, left, right):
        # Note that date subtraction casts arguments to integer. This is to mimic pandas's
        # behaviors. pandas returns 'timedelta64[ns]' in days from date's subtraction.
        msg = (
            "Note that there is a behavior difference of date subtraction. "
            "The date subtraction returns an integer in days, "
            "whereas pandas returns 'timedelta64[ns]'."
        )
        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, DateType):
            warnings.warn(msg, UserWarning)
            return column_op(F.datediff)(left, right).astype("long")
        elif isinstance(right, datetime.date) and not isinstance(right, datetime.datetime):
            warnings.warn(msg, UserWarning)
            return column_op(F.datediff)(left, F.lit(right)).astype("long")
        else:
            raise TypeError("date subtraction can only be applied to date series.")

    def __mul__(self, left, right):
        raise TypeError("multiplication can not be applied to date.")

    def __truediv__(self, left, right):
        raise TypeError("division can not be applied to date.")

    def __floordiv__(self, left, right):
        raise TypeError("division can not be applied to date.")

    def __mod__(self, left, right):
        raise TypeError("modulo can not be applied to date.")

    def __pow__(self, left, right):
        raise TypeError("exponentiation can not be applied to date.")

    def __radd__(self, left, right=None):
        raise TypeError("addition can not be applied to date.")

    def __rsub__(self, left, right=None):
        # Note that date subtraction casts arguments to integer. This is to mimic pandas's
        # behaviors. pandas returns 'timedelta64[ns]' in days from date's subtraction.
        msg = (
            "Note that there is a behavior difference of date subtraction. "
            "The date subtraction returns an integer in days, "
            "whereas pandas returns 'timedelta64[ns]'."
        )
        if isinstance(right, datetime.date) and not isinstance(right, datetime.datetime):
            warnings.warn(msg, UserWarning)
            return -column_op(F.datediff)(left, F.lit(right)).astype("long")
        else:
            raise TypeError("date subtraction can only be applied to date series.")

    def __rmul__(self, left, right=None):
        raise TypeError("multiplication can not be applied to date.")

    def __rtruediv__(self, left, right=None):
        raise TypeError("division can not be applied to date.")

    def __rfloordiv__(self, left, right=None):
        raise TypeError("division can not be applied to date.")

    def __rpow__(self, left, right=None):
        raise TypeError("exponentiation can not be applied to date.")

    def __rmod__(self, left, right=None):
        raise TypeError("modulo can not be applied to date.")

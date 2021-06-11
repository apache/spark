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
from typing import TYPE_CHECKING, Union

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from pyspark.pandas.base import IndexOpsMixin
from pyspark.pandas.data_type_ops.base import DataTypeOps
from pyspark.pandas.typedef import as_spark_type

if TYPE_CHECKING:
    from pyspark.pandas.indexes import Index  # noqa: F401 (SPARK-34943)
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)


class DatetimeOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with spark type: TimestampType.
    """

    @property
    def pretty_name(self) -> str:
        return "datetimes"

    def sub(self, left, right) -> Union["Series", "Index"]:
        # Note that timestamp subtraction casts arguments to integer. This is to mimic pandas's
        # behaviors. pandas returns 'timedelta64[ns]' from 'datetime64[ns]'s subtraction.
        msg = (
            "Note that there is a behavior difference of timestamp subtraction. "
            "The timestamp subtraction returns an integer in seconds, "
            "whereas pandas returns 'timedelta64[ns]'."
        )
        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, TimestampType):
            warnings.warn(msg, UserWarning)
            return left.astype("long") - right.astype("long")
        elif isinstance(right, datetime.datetime):
            warnings.warn(msg, UserWarning)
            return left.astype("long").spark.transform(
                lambda scol: scol - F.lit(right).cast(as_spark_type("long"))
            )
        else:
            raise TypeError("datetime subtraction can only be applied to datetime series.")

    def rsub(self, left, right) -> Union["Series", "Index"]:
        # Note that timestamp subtraction casts arguments to integer. This is to mimic pandas's
        # behaviors. pandas returns 'timedelta64[ns]' from 'datetime64[ns]'s subtraction.
        msg = (
            "Note that there is a behavior difference of timestamp subtraction. "
            "The timestamp subtraction returns an integer in seconds, "
            "whereas pandas returns 'timedelta64[ns]'."
        )
        if isinstance(right, datetime.datetime):
            warnings.warn(msg, UserWarning)
            return -(left.astype("long")).spark.transform(
                lambda scol: scol - F.lit(right).cast(as_spark_type("long"))
            )
        else:
            raise TypeError("datetime subtraction can only be applied to datetime series.")

    def prepare(self, col):
        """Prepare column when from_pandas."""
        return col

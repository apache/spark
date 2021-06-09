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

from itertools import chain
from typing import TYPE_CHECKING, Union

from pandas.api.types import CategoricalDtype

from pyspark.pandas.data_type_ops.base import DataTypeOps
from pyspark.pandas.internal import InternalField
from pyspark.pandas.typedef import Dtype, extension_dtypes, pandas_on_spark_type
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType

if TYPE_CHECKING:
    from pyspark.pandas.indexes import Index  # noqa: F401 (SPARK-34943)
    from pyspark.pandas.series import Series  # noqa: F401 (SPARK-34943)


class NullOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with Spark type: NullType.
    """

    @property
    def pretty_name(self) -> str:
        return "nulls"

    def astype(
        self, index_ops: Union["Index", "Series"], dtype: Union[str, type, Dtype]
    ) -> Union["Index", "Series"]:
        dtype, spark_type = pandas_on_spark_type(dtype)
        if not spark_type:
            raise ValueError("Type {} not understood".format(dtype))

        if isinstance(dtype, CategoricalDtype):
            if dtype.categories is None:
                codes, uniques = index_ops.factorize()
                return codes._with_new_scol(
                    codes.spark.column,
                    field=codes._internal.data_fields[0].copy(
                        dtype=CategoricalDtype(categories=uniques)
                    ),
                )
            else:
                categories = dtype.categories
                if len(categories) == 0:
                    scol = F.lit(-1)
                else:
                    kvs = chain(
                        *[
                            (F.lit(category), F.lit(code))
                            for code, category in enumerate(categories)
                        ]
                    )
                    map_scol = F.create_map(*kvs)

                    scol = F.coalesce(map_scol.getItem(index_ops.spark.column), F.lit(-1))
                return index_ops._with_new_scol(
                    scol.cast(spark_type).alias(index_ops._internal.data_fields[0].name),
                    field=index_ops._internal.data_fields[0].copy(
                        dtype=dtype, spark_type=spark_type, nullable=False
                    ),
                )

        if isinstance(spark_type, BooleanType):
            if isinstance(dtype, extension_dtypes):
                scol = index_ops.spark.column.cast(spark_type)
            else:
                scol = F.when(index_ops.spark.column.isNull(), F.lit(False)).otherwise(
                    index_ops.spark.column.cast(spark_type)
                )
        elif isinstance(spark_type, StringType):
            if isinstance(dtype, extension_dtypes):
                scol = index_ops.spark.column.cast(spark_type)
            else:
                null_str = str(None)
                casted = index_ops.spark.column.cast(spark_type)
                scol = F.when(index_ops.spark.column.isNull(), null_str).otherwise(casted)
        else:
            scol = index_ops.spark.column.cast(spark_type)
        return index_ops._with_new_scol(
            scol.alias(index_ops._internal.data_spark_column_names[0]),
            field=InternalField(dtype=dtype),
        )

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

from pyspark.sql.dataframe import DataFrame
from pyspark.rdd import PythonEvalType
from pyspark.sql.column import Column


class CoGroupedData(object):

    def __init__(self, gd1, gd2):
        self._gd1 = gd1
        self._gd2 = gd2
        self.sql_ctx = gd1.sql_ctx

    def apply(self, udf):
        # Columns are special because hasattr always return True
        if isinstance(udf, Column) or not hasattr(udf, 'func') \
           or udf.evalType != PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
            raise ValueError("Invalid udf: the udf argument must be a pandas_udf of type "
                             "COGROUPED_MAP.")
        all_cols = self._extract_cols(self._gd1) + self._extract_cols(self._gd2)
        udf_column = udf(*all_cols)
        jdf = self._gd1._jgd.flatMapCoGroupsInPandas(self._gd2._jgd, udf_column._jc.expr())
        return DataFrame(jdf, self.sql_ctx)

    @staticmethod
    def _extract_cols(gd):
        df = gd._df
        return [df[col] for col in df.columns]


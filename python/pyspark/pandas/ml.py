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

from typing import List, Tuple, TYPE_CHECKING, cast

import numpy as np
import pandas as pd
import pyspark

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

from pyspark.pandas._typing import Label
from pyspark.pandas.utils import column_labels_level

if TYPE_CHECKING:
    import pyspark.pandas as ps


CORRELATION_OUTPUT_COLUMN = "__correlation_output__"


def corr(psdf: "ps.DataFrame", method: str = "pearson") -> pd.DataFrame:
    """
    The correlation matrix of all the numerical columns of this dataframe.

    Only accepts scalar numerical values for now.

    :param psdf: the pandas-on-Spark dataframe.
    :param method: {'pearson', 'spearman'}
                   * pearson : standard correlation coefficient
                   * spearman : Spearman rank correlation
    :return: :class:`pandas.DataFrame`

    >>> ps.DataFrame({'A': [0, 1], 'B': [1, 0], 'C': ['x', 'y']}).corr()
         A    B
    A  1.0 -1.0
    B -1.0  1.0
    """
    assert method in ("pearson", "spearman")
    ndf, column_labels = to_numeric_df(psdf)
    corr = Correlation.corr(ndf, CORRELATION_OUTPUT_COLUMN, method)
    pcorr = cast(pd.DataFrame, corr.toPandas())
    arr = pcorr.iloc[0, 0].toArray()
    if column_labels_level(column_labels) > 1:
        idx = pd.MultiIndex.from_tuples(column_labels)
    else:
        idx = pd.Index([label[0] for label in column_labels])
    return pd.DataFrame(arr, columns=idx, index=idx)


def to_numeric_df(psdf: "ps.DataFrame") -> Tuple[pyspark.sql.DataFrame, List[Label]]:
    """
    Takes a dataframe and turns it into a dataframe containing a single numerical
    vector of doubles. This dataframe has a single field called '_1'.

    TODO: index is not preserved currently
    :param psdf: the pandas-on-Spark dataframe.
    :return: a pair of dataframe, list of strings (the name of the columns
             that were converted to numerical types)

    >>> to_numeric_df(ps.DataFrame({'A': [0, 1], 'B': [1, 0], 'C': ['x', 'y']}))
    (DataFrame[__correlation_output__: vector], [('A',), ('B',)])
    """
    # TODO, it should be more robust.
    accepted_types = {
        np.dtype(dt)  # type: ignore[misc]
        for dt in [np.int8, np.int16, np.int32, np.int64, np.float32, np.float64, np.bool_]
    }
    numeric_column_labels = [
        label for label in psdf._internal.column_labels if psdf[label].dtype in accepted_types
    ]
    numeric_df = psdf._internal.spark_frame.select(
        *[psdf._internal.spark_column_for(idx) for idx in numeric_column_labels]
    )
    va = VectorAssembler(inputCols=numeric_df.columns, outputCol=CORRELATION_OUTPUT_COLUMN)
    v = va.transform(numeric_df).select(CORRELATION_OUTPUT_COLUMN)
    return v, numeric_column_labels


def _test() -> None:
    import os
    import doctest
    import sys
    from pyspark.sql import SparkSession
    import pyspark.pandas.ml

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.pandas.ml.__dict__.copy()
    globs["ps"] = pyspark.pandas
    spark = SparkSession.builder.master("local[4]").appName("pyspark.pandas.ml tests").getOrCreate()
    (failure_count, test_count) = doctest.testmod(
        pyspark.pandas.ml, globs=globs, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()

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


from pyspark.pandas.missing import unsupported_function, unsupported_property


def _unsupported_function(method_name, deprecated=False, reason=""):
    return unsupported_function(
        class_name="pd.resample.Resampler",
        method_name=method_name,
        deprecated=deprecated,
        reason=reason,
    )


def _unsupported_property(property_name, deprecated=False, reason=""):
    return unsupported_property(
        class_name="pd.resample.Resampler",
        property_name=property_name,
        deprecated=deprecated,
        reason=reason,
    )


class MissingPandasLikeDataFrameResampler:
    # NOTE: Please update the pandas-on-Spark reference document when implementing the new API.
    # Documentation path: `python/docs/source/reference/pyspark.pandas/`.

    # Properties
    groups = _unsupported_property("groups")
    indices = _unsupported_property("indices")

    # Functions
    get_group = _unsupported_property("get_group")
    apply = _unsupported_function("apply")
    aggregate = _unsupported_function("aggregate")
    transform = _unsupported_function("transform")
    pipe = _unsupported_function("pipe")
    ffill = _unsupported_function("ffill")
    backfill = _unsupported_function("backfill")
    bfill = _unsupported_function("bfill")
    pad = _unsupported_function("pad")
    nearest = _unsupported_function("nearest")
    fillna = _unsupported_function("fillna")
    asfreq = _unsupported_function("asfreq")
    interpolate = _unsupported_function("interpolate")
    count = _unsupported_function("count")
    nunique = _unsupported_function("nunique")
    first = _unsupported_function("first")
    last = _unsupported_function("last")
    median = _unsupported_function("median")
    ohlc = _unsupported_function("ohlc")
    prod = _unsupported_function("prod")
    size = _unsupported_function("size")
    sem = _unsupported_function("sem")
    quantile = _unsupported_function("quantile")


class MissingPandasLikeSeriesResampler:
    # NOTE: Please update the pandas-on-Spark reference document when implementing the new API.
    # Documentation path: `python/docs/source/reference/pyspark.pandas/`.

    # Properties
    groups = _unsupported_property("groups")
    indices = _unsupported_property("indices")

    # Functions
    get_group = _unsupported_property("get_group")
    apply = _unsupported_function("apply")
    aggregate = _unsupported_function("aggregate")
    transform = _unsupported_function("transform")
    pipe = _unsupported_function("pipe")
    ffill = _unsupported_function("ffill")
    backfill = _unsupported_function("backfill")
    bfill = _unsupported_function("bfill")
    pad = _unsupported_function("pad")
    nearest = _unsupported_function("nearest")
    fillna = _unsupported_function("fillna")
    asfreq = _unsupported_function("asfreq")
    interpolate = _unsupported_function("interpolate")
    count = _unsupported_function("count")
    nunique = _unsupported_function("nunique")
    first = _unsupported_function("first")
    last = _unsupported_function("last")
    median = _unsupported_function("median")
    ohlc = _unsupported_function("ohlc")
    prod = _unsupported_function("prod")
    size = _unsupported_function("size")
    sem = _unsupported_function("sem")
    quantile = _unsupported_function("quantile")

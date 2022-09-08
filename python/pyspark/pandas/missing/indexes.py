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
from pyspark.pandas.missing import unsupported_function, unsupported_property, common


def _unsupported_function(method_name, deprecated=False, reason="", cls="Index"):
    return unsupported_function(
        class_name="pd.{}".format(cls),
        method_name=method_name,
        deprecated=deprecated,
        reason=reason,
    )


def _unsupported_property(property_name, deprecated=False, reason="", cls="Index"):
    return unsupported_property(
        class_name="pd.{}".format(cls),
        property_name=property_name,
        deprecated=deprecated,
        reason=reason,
    )


class MissingPandasLikeIndex:
    # NOTE: Please update the pandas-on-Spark reference document when implementing the new API.
    # Documentation path: `python/docs/source/reference/pyspark.pandas/`.

    # Properties
    nbytes = _unsupported_property("nbytes")

    # Functions
    argsort = _unsupported_function("argsort")
    asof_locs = _unsupported_function("asof_locs")
    format = _unsupported_function("format")
    get_indexer = _unsupported_function("get_indexer")
    get_indexer_for = _unsupported_function("get_indexer_for")
    get_indexer_non_unique = _unsupported_function("get_indexer_non_unique")
    get_loc = _unsupported_function("get_loc")
    get_slice_bound = _unsupported_function("get_slice_bound")
    get_value = _unsupported_function("get_value")
    groupby = _unsupported_function("groupby")
    is_ = _unsupported_function("is_")
    join = _unsupported_function("join")
    putmask = _unsupported_function("putmask")
    ravel = _unsupported_function("ravel")
    reindex = _unsupported_function("reindex")
    searchsorted = _unsupported_function("searchsorted")
    slice_indexer = _unsupported_function("slice_indexer")
    slice_locs = _unsupported_function("slice_locs")
    sortlevel = _unsupported_function("sortlevel")
    to_flat_index = _unsupported_function("to_flat_index")
    where = _unsupported_function("where")
    is_mixed = _unsupported_function("is_mixed")

    # Deprecated functions
    set_value = _unsupported_function("set_value", deprecated=True)
    to_native_types = _unsupported_function("to_native_types", deprecated=True)

    # Properties we won't support.
    array = common.array(_unsupported_property)
    duplicated = common.duplicated(_unsupported_property)

    # Functions we won't support.
    memory_usage = common.memory_usage(_unsupported_function)
    __iter__ = common.__iter__(_unsupported_function)


class MissingPandasLikeDatetimeIndex(MissingPandasLikeIndex):
    # NOTE: Please update the pandas-on-Spark reference document when implementing the new API.
    # Documentation path: `python/docs/source/reference/pyspark.pandas/`.

    # Properties
    nanosecond = _unsupported_property("nanosecond", cls="DatetimeIndex")
    date = _unsupported_property("date", cls="DatetimeIndex")
    time = _unsupported_property("time", cls="DatetimeIndex")
    timetz = _unsupported_property("timetz", cls="DatetimeIndex")
    tz = _unsupported_property("tz", cls="DatetimeIndex")
    freq = _unsupported_property("freq", cls="DatetimeIndex")
    freqstr = _unsupported_property("freqstr", cls="DatetimeIndex")
    inferred_freq = _unsupported_property("inferred_freq", cls="DatetimeIndex")

    # Functions
    snap = _unsupported_function("snap", cls="DatetimeIndex")
    tz_convert = _unsupported_function("tz_convert", cls="DatetimeIndex")
    tz_localize = _unsupported_function("tz_localize", cls="DatetimeIndex")
    to_period = _unsupported_function("to_period", cls="DatetimeIndex")
    to_perioddelta = _unsupported_function("to_perioddelta", cls="DatetimeIndex")
    to_pydatetime = _unsupported_function("to_pydatetime", cls="DatetimeIndex")
    mean = _unsupported_function("mean", cls="DatetimeIndex")
    std = _unsupported_function("std", cls="DatetimeIndex")


class MissingPandasLikeTimedeltaIndex(MissingPandasLikeIndex):
    # NOTE: Please update the pandas-on-Spark reference document when implementing the new API.
    # Documentation path: `python/docs/source/reference/pyspark.pandas/`.

    # Properties
    nanoseconds = _unsupported_property("nanoseconds", cls="TimedeltaIndex")
    components = _unsupported_property("components", cls="TimedeltaIndex")
    inferred_freq = _unsupported_property("inferred_freq", cls="TimedeltaIndex")

    # Functions
    to_pytimedelta = _unsupported_function("to_pytimedelta", cls="TimedeltaIndex")
    round = _unsupported_function("round", cls="TimedeltaIndex")
    floor = _unsupported_function("floor", cls="TimedeltaIndex")
    ceil = _unsupported_function("ceil", cls="TimedeltaIndex")
    mean = _unsupported_function("mean", cls="TimedeltaIndex")


class MissingPandasLikeMultiIndex:
    # NOTE: Please update the pandas-on-Spark reference document when implementing the new API.
    # Documentation path: `python/docs/source/reference/pyspark.pandas/`.

    # Functions
    argsort = _unsupported_function("argsort")
    asof_locs = _unsupported_function("asof_locs")
    factorize = _unsupported_function("factorize")
    format = _unsupported_function("format")
    get_indexer = _unsupported_function("get_indexer")
    get_indexer_for = _unsupported_function("get_indexer_for")
    get_indexer_non_unique = _unsupported_function("get_indexer_non_unique")
    get_loc = _unsupported_function("get_loc")
    get_loc_level = _unsupported_function("get_loc_level")
    get_locs = _unsupported_function("get_locs")
    get_slice_bound = _unsupported_function("get_slice_bound")
    get_value = _unsupported_function("get_value")
    groupby = _unsupported_function("groupby")
    is_ = _unsupported_function("is_")
    is_lexsorted = _unsupported_function("is_lexsorted")
    join = _unsupported_function("join")
    map = _unsupported_function("map")
    putmask = _unsupported_function("putmask")
    ravel = _unsupported_function("ravel")
    reindex = _unsupported_function("reindex")
    remove_unused_levels = _unsupported_function("remove_unused_levels")
    reorder_levels = _unsupported_function("reorder_levels")
    searchsorted = _unsupported_function("searchsorted")
    set_codes = _unsupported_function("set_codes")
    set_levels = _unsupported_function("set_levels")
    slice_indexer = _unsupported_function("slice_indexer")
    slice_locs = _unsupported_function("slice_locs")
    sortlevel = _unsupported_function("sortlevel")
    to_flat_index = _unsupported_function("to_flat_index")
    truncate = _unsupported_function("truncate")
    where = _unsupported_function("where")

    # Deprecated functions
    is_mixed = _unsupported_function(
        "is_mixed", deprecated=True, reason="Check index.inferred_type directly instead."
    )
    set_value = _unsupported_function("set_value", deprecated=True)
    to_native_types = _unsupported_function("to_native_types", deprecated=True)

    # Functions we won't support.
    array = common.array(_unsupported_property)
    duplicated = common.duplicated(_unsupported_property)
    codes = _unsupported_property(
        "codes",
        reason="'codes' requires to collect all data into the driver which is against the "
        "design principle of pandas-on-Spark. Alternatively, you could call 'to_pandas()' and"
        " use 'codes' property in pandas.",
    )
    levels = _unsupported_property(
        "levels",
        reason="'levels' requires to collect all data into the driver which is against the "
        "design principle of pandas-on-Spark. Alternatively, you could call 'to_pandas()' and"
        " use 'levels' property in pandas.",
    )
    __iter__ = common.__iter__(_unsupported_function)

    # Properties we won't support.
    memory_usage = common.memory_usage(_unsupported_function)

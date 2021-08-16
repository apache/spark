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
from distutils.version import LooseVersion

import pandas as pd

from pyspark.pandas.missing import unsupported_function, unsupported_property, common


def _unsupported_function(method_name, deprecated=False, reason=""):
    return unsupported_function(
        class_name="pd.Series", method_name=method_name, deprecated=deprecated, reason=reason
    )


def _unsupported_property(property_name, deprecated=False, reason=""):
    return unsupported_property(
        class_name="pd.Series", property_name=property_name, deprecated=deprecated, reason=reason
    )


class MissingPandasLikeSeries(object):

    # Functions
    asfreq = _unsupported_function("asfreq")
    autocorr = _unsupported_function("autocorr")
    combine = _unsupported_function("combine")
    convert_dtypes = _unsupported_function("convert_dtypes")
    ewm = _unsupported_function("ewm")
    infer_objects = _unsupported_function("infer_objects")
    interpolate = _unsupported_function("interpolate")
    reorder_levels = _unsupported_function("reorder_levels")
    resample = _unsupported_function("resample")
    searchsorted = _unsupported_function("searchsorted")
    set_axis = _unsupported_function("set_axis")
    slice_shift = _unsupported_function("slice_shift")
    to_hdf = _unsupported_function("to_hdf")
    to_period = _unsupported_function("to_period")
    to_sql = _unsupported_function("to_sql")
    to_timestamp = _unsupported_function("to_timestamp")
    tshift = _unsupported_function("tshift")
    tz_convert = _unsupported_function("tz_convert")
    tz_localize = _unsupported_function("tz_localize")
    view = _unsupported_function("view")

    # Deprecated functions
    convert_objects = _unsupported_function("convert_objects", deprecated=True)
    nonzero = _unsupported_function("nonzero", deprecated=True)
    reindex_axis = _unsupported_function("reindex_axis", deprecated=True)
    select = _unsupported_function("select", deprecated=True)
    get_values = _unsupported_function("get_values", deprecated=True)

    # Properties we won't support.
    array = common.array(_unsupported_property)
    duplicated = common.duplicated(_unsupported_property)
    nbytes = _unsupported_property(
        "nbytes",
        reason="'nbytes' requires to compute whole dataset. You can calculate manually it, "
        "with its 'itemsize', by explicitly executing its count. Use Spark's web UI "
        "to monitor disk and memory usage of your application in general.",
    )

    # Functions we won't support.
    memory_usage = common.memory_usage(_unsupported_function)
    to_pickle = common.to_pickle(_unsupported_function)
    to_xarray = common.to_xarray(_unsupported_function)
    __iter__ = common.__iter__(_unsupported_function)
    ravel = _unsupported_function(
        "ravel",
        reason="If you want to collect your flattened underlying data as an NumPy array, "
        "use 'to_numpy().ravel()' instead.",
    )

    if LooseVersion(pd.__version__) < LooseVersion("1.0"):
        # Deprecated properties
        blocks = _unsupported_property("blocks", deprecated=True)
        ftypes = _unsupported_property("ftypes", deprecated=True)
        ftype = _unsupported_property("ftype", deprecated=True)
        is_copy = _unsupported_property("is_copy", deprecated=True)
        ix = _unsupported_property("ix", deprecated=True)
        asobject = _unsupported_property("asobject", deprecated=True)
        strides = _unsupported_property("strides", deprecated=True)
        imag = _unsupported_property("imag", deprecated=True)
        itemsize = _unsupported_property("itemsize", deprecated=True)
        data = _unsupported_property("data", deprecated=True)
        base = _unsupported_property("base", deprecated=True)
        flags = _unsupported_property("flags", deprecated=True)

        # Deprecated functions
        as_blocks = _unsupported_function("as_blocks", deprecated=True)
        as_matrix = _unsupported_function("as_matrix", deprecated=True)
        clip_lower = _unsupported_function("clip_lower", deprecated=True)
        clip_upper = _unsupported_function("clip_upper", deprecated=True)
        compress = _unsupported_function("compress", deprecated=True)
        get_ftype_counts = _unsupported_function("get_ftype_counts", deprecated=True)
        get_value = _unsupported_function("get_value", deprecated=True)
        set_value = _unsupported_function("set_value", deprecated=True)
        valid = _unsupported_function("valid", deprecated=True)
        to_dense = _unsupported_function("to_dense", deprecated=True)
        to_sparse = _unsupported_function("to_sparse", deprecated=True)
        to_msgpack = _unsupported_function("to_msgpack", deprecated=True)
        compound = _unsupported_function("compound", deprecated=True)
        put = _unsupported_function("put", deprecated=True)
        ptp = _unsupported_function("ptp", deprecated=True)

        # Functions we won't support.
        real = _unsupported_property(
            "real",
            reason="If you want to collect your data as an NumPy array, use 'to_numpy()' instead.",
        )

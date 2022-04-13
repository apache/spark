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


def _unsupported_function(method_name, deprecated=False, reason=""):
    return unsupported_function(
        class_name="pd.Series", method_name=method_name, deprecated=deprecated, reason=reason
    )


def _unsupported_property(property_name, deprecated=False, reason=""):
    return unsupported_property(
        class_name="pd.Series", property_name=property_name, deprecated=deprecated, reason=reason
    )


class MissingPandasLikeSeries:

    # Functions
    asfreq = _unsupported_function("asfreq")
    autocorr = _unsupported_function("autocorr")
    combine = _unsupported_function("combine")
    convert_dtypes = _unsupported_function("convert_dtypes")
    infer_objects = _unsupported_function("infer_objects")
    interpolate = _unsupported_function("interpolate")
    reorder_levels = _unsupported_function("reorder_levels")
    resample = _unsupported_function("resample")
    searchsorted = _unsupported_function("searchsorted")
    set_axis = _unsupported_function("set_axis")
    to_hdf = _unsupported_function("to_hdf")
    to_period = _unsupported_function("to_period")
    to_sql = _unsupported_function("to_sql")
    to_timestamp = _unsupported_function("to_timestamp")
    tz_convert = _unsupported_function("tz_convert")
    tz_localize = _unsupported_function("tz_localize")
    view = _unsupported_function("view")

    # Deprecated functions
    slice_shift = _unsupported_function(
        "slice_shift", deprecated=True, reason="Use DataFrame/Series.shift instead."
    )
    tshift = _unsupported_function("tshift", deprecated=True, reason="Use `shift` instead.")

    # Properties we won't support.
    array = common.array(_unsupported_property)
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

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
        class_name="pd.DataFrame", method_name=method_name, deprecated=deprecated, reason=reason
    )


def _unsupported_property(property_name, deprecated=False, reason=""):
    return unsupported_property(
        class_name="pd.DataFrame", property_name=property_name, deprecated=deprecated, reason=reason
    )


class _MissingPandasLikeDataFrame:
    # NOTE: Please update the document "Supported pandas APIs" when implementing the new API.
    # Documentation path: `python/docs/source/user_guide/pandas_on_spark/supported_pandas_api.rst`.

    # Functions
    asfreq = _unsupported_function("asfreq")
    asof = _unsupported_function("asof")
    boxplot = _unsupported_function("boxplot")
    combine = _unsupported_function("combine")
    compare = _unsupported_function("compare")
    convert_dtypes = _unsupported_function("convert_dtypes")
    corrwith = _unsupported_function("corrwith")
    ewm = _unsupported_function("ewm")
    infer_objects = _unsupported_function("infer_objects")
    interpolate = _unsupported_function("interpolate")
    mode = _unsupported_function("mode")
    reorder_levels = _unsupported_function("reorder_levels")
    resample = _unsupported_function("resample")
    set_axis = _unsupported_function("set_axis")
    to_feather = _unsupported_function("to_feather")
    to_gbq = _unsupported_function("to_gbq")
    to_hdf = _unsupported_function("to_hdf")
    to_period = _unsupported_function("to_period")
    to_sql = _unsupported_function("to_sql")
    to_stata = _unsupported_function("to_stata")
    to_timestamp = _unsupported_function("to_timestamp")
    tz_convert = _unsupported_function("tz_convert")
    tz_localize = _unsupported_function("tz_localize")

    # Deprecated functions
    tshift = _unsupported_function("tshift", deprecated=True, reason="Please use shift instead.")
    slice_shift = _unsupported_function(
        "slice_shift", deprecated=True, reason="You can use DataFrame/Series.shift instead."
    )
    lookup = _unsupported_function(
        "lookup", deprecated=True, reason="Use DataFrame.melt and DataFrame.loc instead."
    )

    # Functions we won't support.
    to_pickle = common.to_pickle(_unsupported_function)
    memory_usage = common.memory_usage(_unsupported_function)
    to_xarray = common.to_xarray(_unsupported_function)

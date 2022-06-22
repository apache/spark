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
from pyspark.pandas.missing import unsupported_function


def _unsupported_function(method_name, deprecated=False, reason=""):
    return unsupported_function(
        class_name="pd", method_name=method_name, deprecated=deprecated, reason=reason
    )


class _MissingPandasLikeGeneralFunctions:

    pivot = _unsupported_function("pivot")
    pivot_table = _unsupported_function("pivot_table")
    crosstab = _unsupported_function("crosstab")
    cut = _unsupported_function("cut")
    qcut = _unsupported_function("qcut")
    merge_ordered = _unsupported_function("merge_ordered")
    factorize = _unsupported_function("factorize")
    unique = _unsupported_function("unique")
    wide_to_long = _unsupported_function("wide_to_long")
    bdate_range = _unsupported_function("bdate_range")
    period_range = _unsupported_function("period_range")
    infer_freq = _unsupported_function("infer_freq")
    interval_range = _unsupported_function("interval_range")
    eval = _unsupported_function("eval")

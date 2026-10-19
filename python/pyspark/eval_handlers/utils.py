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

"""Shared helpers for the eval type handlers.

A leaf module (no pyspark imports), so it is safe to import from both the worker
and the handlers, on the driver and the executor alike.
"""

from typing import Any


def extract_key_value_indexes(grouped_arg_offsets: list) -> list:
    """
    Extract the key and value indexes from arg_offsets for the grouped and cogrouped grouped-map
    udfs. See BasePandasGroupExec.resolveArgOffsets for equivalent scala code.

    Parameters
    ----------
    grouped_arg_offsets:  list
        List containing the key and value indexes of columns of the
        DataFrames to be passed to the udf. It consists of n repeating groups where n is the
        number of DataFrames.  Each group has the following format:
            group[0]: length of group
            group[1]: length of key indexes
            group[2.. group[1] +2]: key attributes
            group[group[1] +3 group[0]]: value attributes
    """
    parsed = []
    idx = 0
    while idx < len(grouped_arg_offsets):
        offsets_len = grouped_arg_offsets[idx]
        idx += 1
        offsets = grouped_arg_offsets[idx : idx + offsets_len]
        split_index = offsets[0] + 1
        offset_keys = offsets[1:split_index]
        offset_values = offsets[split_index:]
        parsed.append([offset_keys, offset_values])
        idx += offsets_len
    return parsed


# Sentinel standing in for NaN grouping-key values in the map-side incremental-aggregate combine.
# ``float('nan') != float('nan')``, so distinct NaN objects would never collide in a dict; mapping
# them to one sentinel lets the map-side combine collapse them (correctness does not depend on it,
# as the FINAL stage re-groups authoritatively).
_NAN_GROUPING_KEY = object()


def hashable_grouping_key(key_values: tuple[Any, ...]) -> Any:
    """
    Canonicalize a grouping-key value tuple (extracted from Arrow via ``to_pylist``) into a
    hashable form usable as a ``dict`` key for the map-side PARTIAL combine of incremental Python
    aggregators.

    Lists (from ``array`` columns) become tuples and dicts (from ``struct`` columns) become tuples
    of ``(name, value)`` pairs, so nested complex keys hash by value; NaN floats map to a sentinel.
    This is a best-effort combine only: an exotic unhashable value falls back to a unique object so
    the row forms its own group, and the FINAL stage merges any keys left uncollapsed here.
    """

    def canon(v: Any) -> Any:
        if isinstance(v, float) and v != v:
            return _NAN_GROUPING_KEY
        if isinstance(v, list):
            return tuple(canon(x) for x in v)
        if isinstance(v, dict):
            return tuple((k, canon(x)) for k, x in v.items())
        return v

    try:
        canonical = tuple(canon(v) for v in key_values)
        hash(canonical)
        return canonical
    except TypeError:
        return object()

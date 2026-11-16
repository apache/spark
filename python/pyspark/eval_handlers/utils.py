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

import datetime
import decimal
import sys
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    import pyarrow as pa


def extract_key_value_indexes(grouped_arg_offsets: list) -> list:
    """
    Extract the key and value indexes from arg_offsets for the grouped and cogrouped grouped-map
    udfs. See PandasGroupUtils.resolveArgOffsets for equivalent scala code.

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


def wrap_kwargs_support(
    f: Callable[..., Any], args_offsets: list[int], kwargs_offsets: dict[str, int]
) -> tuple[Callable[..., Any], list[int]]:
    """
    Wrap ``f`` so it takes all arguments positionally: the positional ones first, then the
    keyword ones in ``kwargs_offsets`` order. Returns the wrapped function and the matching
    combined offsets.
    """
    if len(kwargs_offsets):
        keys = list(kwargs_offsets.keys())

        len_args_offsets = len(args_offsets)
        if len_args_offsets > 0:

            def func(*args: Any) -> Any:
                return f(*args[:len_args_offsets], **dict(zip(keys, args[len_args_offsets:])))

        else:

            def func(*args: Any) -> Any:
                return f(**dict(zip(keys, args)))

        return func, args_offsets + [kwargs_offsets[key] for key in keys]
    else:
        return f, args_offsets


# Python scalar types whose values are immutable; see ``is_immutable_python_value``.
_IMMUTABLE_PYTHON_SCALAR_TYPES = (
    type(None),
    bool,
    int,
    float,
    str,
    bytes,
    decimal.Decimal,
    datetime.date,
    datetime.time,
    datetime.timedelta,
)
_IMMUTABLE_PYTHON_SCALAR_TYPE_SET = frozenset(_IMMUTABLE_PYTHON_SCALAR_TYPES + (datetime.datetime,))


def is_immutable_python_value(value: Any) -> bool:
    """
    Whether nothing can update ``value`` in place: ``None``, a ``bool``, ``int``, ``float``,
    ``str``, ``bytes``, ``Decimal``, date, time or timedelta, or a subclass of one (such as a
    numpy ``float64``), a numpy scalar other than ``numpy.void``, or a tuple of such values,
    including a ``Row`` or a namedtuple, whose items are all that Arrow reads of it. A ``list``,
    ``dict``, set, ``bytearray``, ``numpy.void`` or numpy array (even 0-d) can be updated in place.
    """
    if type(value) in _IMMUTABLE_PYTHON_SCALAR_TYPE_SET:
        return True
    if isinstance(value, tuple):
        return all(is_immutable_python_value(v) for v in value)
    if isinstance(value, _IMMUTABLE_PYTHON_SCALAR_TYPES):
        return True
    # A numpy scalar, but not a structured one (``numpy.void``), which can be a view of an array.
    numpy = sys.modules.get("numpy")
    return (
        numpy is not None and isinstance(value, numpy.generic) and not isinstance(value, numpy.void)
    )


# No result yet; see ``WindowAggResults._unchecked``.
_NO_RESULT = object()


class WindowAggResults:
    """
    Collects the per-row ``finish`` results of one window aggregate into an Arrow array.

    A result can share state with the buffer that it was finished from, e.g. the list that
    ``reduce`` appends to, and ``reduce`` may update that buffer in place for later rows. The
    results are held as Python values and converted in batches: ``flush``, called before
    ``reduce`` updates the buffer that the pending results were finished from, converts them if
    one of them may be updated in place (see ``is_immutable_python_value``). A result finished
    from a buffer just folded from ``zero`` is checked only if such a ``flush`` follows, which a
    sliding frame mostly skips. Every ``_COMBINE_CHUNKS`` such chunks are combined into one
    array, so that the per-array overhead of the small chunks stays bounded.
    """

    _COMBINE_CHUNKS = 1024

    def __init__(self, arrow_type: "pa.DataType") -> None:
        self._arrow_type = arrow_type
        self._pending: list[Any] = []
        self._pending_may_share_state = False
        # The result finished from a buffer just folded from ``zero``, not checked yet.
        self._unchecked: Any = _NO_RESULT
        self._chunks: list["pa.Array"] = []  # each combined from ``_COMBINE_CHUNKS`` small chunks
        self._small_chunks: list["pa.Array"] = []

    def append(self, result: Any, refolded: bool = False) -> None:
        """
        Add the next row's result. ``refolded`` says that ``result`` was finished from a buffer
        just folded from ``zero``. Nothing updates the buffers of the earlier results any more
        then, so only ``result`` still needs a check, which ``flush`` makes if it comes.
        """
        self._pending.append(result)
        if refolded:
            self._pending_may_share_state = False
            self._unchecked = result
        elif not self._pending_may_share_state and not is_immutable_python_value(result):
            self._pending_may_share_state = True

    def flush(self) -> None:
        """Convert the pending results if one of them may share state with a buffer."""
        if self._unchecked is not _NO_RESULT:
            if not self._pending_may_share_state and not is_immutable_python_value(self._unchecked):
                self._pending_may_share_state = True
            self._unchecked = _NO_RESULT
        if not self._pending_may_share_state:
            return
        import pyarrow as pa

        self._small_chunks.append(pa.array(self._pending, type=self._arrow_type))
        self._pending = []
        self._pending_may_share_state = False
        if len(self._small_chunks) == self._COMBINE_CHUNKS:
            self._chunks.append(pa.concat_arrays(self._small_chunks))
            self._small_chunks = []

    def to_array(self) -> "pa.Array":
        """All the results, in order, as one array."""
        import pyarrow as pa

        pending = pa.array(self._pending, type=self._arrow_type)
        if not self._chunks and not self._small_chunks:
            return pending
        return pa.concat_arrays(self._chunks + self._small_chunks + [pending])

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Internal helpers for the PySpark StreamTest framework.

These helpers are not part of the public API and may change without notice.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# Simple-name shortcuts accepted by ``MemoryStream(spark, "<name>")``. Mirrors
# the small set of primitive Encoders covered by Scala's
# ``MemoryStream[Int]`` / ``MemoryStream[String]`` / etc.
_SIMPLE_TYPES: Dict[str, Any] = {
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "string": StringType(),
    "str": StringType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
}


def resolve_schema(schema: Union[str, StructType]) -> StructType:
    """Resolve a schema spec into a ``StructType``.

    Accepts either a ``StructType`` or a simple primitive type name (e.g.
    ``"int"``). Primitive names produce a single-column schema with a column
    named ``value``, mirroring Scala's ``MemoryStream[Int]`` (single output
    column ``value``).

    Raises
    ------
    ValueError
        If the string is not a recognized primitive name.
    """
    if isinstance(schema, StructType):
        return schema
    if not isinstance(schema, str):
        raise TypeError(
            f"schema must be a StructType or a primitive type name string, "
            f"got {type(schema).__name__}"
        )
    name = schema.strip().lower()
    if name not in _SIMPLE_TYPES:
        raise ValueError(
            f"Unsupported primitive type {schema!r}. "
            f"Supported names: {sorted(_SIMPLE_TYPES)}. "
            f"Pass a StructType for richer schemas."
        )
    return StructType([StructField("value", _SIMPLE_TYPES[name], True)])


def _object_to_dict(obj: Any, field_names: List[str]) -> Optional[Dict[str, Any]]:
    """Try to extract values for ``field_names`` from an arbitrary object.

    Tried in order:

    1. ``_asdict()`` -- namedtuples and ``typing.NamedTuple``
    2. ``__dict__`` -- plain classes and dataclasses
    3. ``getattr()`` -- slot-based or property-backed attributes

    Returns ``None`` if no strategy yields all required field names. The
    caller is expected to raise a descriptive ``TypeError`` in that case.
    """
    asdict = getattr(obj, "_asdict", None)
    if callable(asdict):
        d = asdict()
        if all(name in d for name in field_names):
            return {name: d[name] for name in field_names}

    obj_dict = getattr(obj, "__dict__", None)
    if isinstance(obj_dict, dict) and all(name in obj_dict for name in field_names):
        return {name: obj_dict[name] for name in field_names}

    try:
        return {name: getattr(obj, name) for name in field_names}
    except AttributeError:
        return None


def to_rows(items: List[Any], schema: StructType) -> List[Row]:
    """Convert a list of input values to ``Row`` objects matching ``schema``.

    This is the Python analogue of Scala's
    ``createToExternalRowConverter[A]()`` used inside ``CheckAnswer`` /
    ``AddData``. Accepted item types:

    * ``Row`` -- kept as-is (no schema validation; the caller is expected to
      build the Row with field names that match the schema).
    * ``dict`` -- keys must include all schema field names.
    * ``tuple`` / ``list`` -- values are positionally assigned to fields.
    * Scalar (int, str, float, bool, ...) -- only valid for single-column
      schemas; wrapped as ``Row(<field>=value)``.
    * Arbitrary objects -- namedtuples, dataclasses, plain classes whose
      ``_asdict()`` / ``__dict__`` / attributes expose all field names.

    Raises
    ------
    TypeError
        If an item cannot be converted (e.g. a scalar with a multi-column
        schema, or an object missing required fields).
    """
    field_names = [f.name for f in schema.fields]
    single_col = len(field_names) == 1
    out: List[Row] = []
    for item in items:
        if isinstance(item, Row):
            out.append(item)
            continue
        if isinstance(item, dict):
            missing = [n for n in field_names if n not in item]
            if missing:
                raise TypeError(
                    f"dict {item!r} is missing required fields {missing} "
                    f"for schema {schema.simpleString()}"
                )
            out.append(Row(**{n: item[n] for n in field_names}))
            continue
        # Namedtuples are tuples but carry field names; resolve them
        # by name so a namedtuple whose fields are in a different order
        # than the schema doesn't silently produce mis-mapped Rows.
        if isinstance(item, tuple) and hasattr(item, "_fields"):
            row_dict = _object_to_dict(item, field_names)
            if row_dict is None:
                raise TypeError(
                    f"namedtuple {item!r} fields {item._fields} do not "
                    f"cover schema fields {field_names}"
                )
            out.append(Row(**row_dict))
            continue
        if isinstance(item, (tuple, list)):
            if len(item) != len(field_names):
                raise TypeError(
                    f"sequence {item!r} has length {len(item)}, expected "
                    f"{len(field_names)} for schema {schema.simpleString()}"
                )
            out.append(Row(**dict(zip(field_names, item))))
            continue
        if single_col:
            out.append(Row(**{field_names[0]: item}))
            continue

        row_dict = _object_to_dict(item, field_names)
        if row_dict is None:
            raise TypeError(
                f"Cannot convert {type(item).__name__} {item!r} to Row for "
                f"schema {schema.simpleString()}. Pass a Row, dict, tuple, "
                f"or an object whose attributes match {field_names}."
            )
        out.append(Row(**row_dict))
    return out

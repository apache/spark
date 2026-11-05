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

"""
The cloudpickle implementation that PySpark uses to serialize Python functions and classes:
`cloudpickle-rust <https://github.com/HyukjinKwon/cloudpickle-rust>`_, a Rust implementation of
cloudpickle with the same API and payloads.

PySpark used to bundle a copy of cloudpickle as this package. Objects pickled by such versions,
for example the UDFs sent by an older Spark Connect client, reference the functions that rebuild
dynamic functions and classes as ``pyspark.cloudpickle.cloudpickle.<name>``. The ``cloudpickle``
and ``cloudpickle_fast`` submodules are therefore aliases of the ones of ``cloudpickle_rust``.
"""

import io
import pickle
import sys
import types
from typing import IO, Any, Callable, Optional, Tuple

import cloudpickle_rust
from cloudpickle_rust import (
    __version__,
    cloudpickle,
    cloudpickle_fast,
    load,
    loads,
    register_pickle_by_value,
    unregister_pickle_by_value,
)

sys.modules[__name__ + ".cloudpickle"] = cloudpickle
sys.modules[__name__ + ".cloudpickle_fast"] = cloudpickle_fast

__all__ = [
    "__version__",
    "Pickler",
    "CloudPickler",
    "dumps",
    "loads",
    "dump",
    "load",
    "register_pickle_by_value",
    "unregister_pickle_by_value",
]

_base_dispatch_table = cloudpickle_rust.Pickler.dispatch_table
_base_code_reduce = _base_dispatch_table[types.CodeType]


def _code_reduce(obj: types.CodeType) -> Tuple[Any, Tuple[Any, ...]]:
    # Like cloudpickle 3.2, cloudpickle-rust sets the filename of the code it pickles by value to
    # "<dynamic-code>", so that the payloads do not depend on where the code is defined. Keep the
    # original filename instead: Spark shows it in the tracebacks of Python UDFs, and the UDF
    # profilers need it to show their results.
    reconstructor, args = _base_code_reduce(obj)
    try:
        index = args.index("<dynamic-code>")
    except ValueError:
        return reconstructor, args
    return reconstructor, args[:index] + (obj.co_filename,) + args[index + 1 :]


class Pickler(cloudpickle_rust.Pickler):
    # Of the same type as the dispatch table of cloudpickle-rust, whose lookups are native.
    dispatch_table = type(_base_dispatch_table)(
        {types.CodeType: _code_reduce}, *_base_dispatch_table.maps
    )


CloudPickler = Pickler


def dump(
    obj: Any,
    file: IO[bytes],
    protocol: Optional[int] = None,
    buffer_callback: Optional[Callable[[pickle.PickleBuffer], Any]] = None,
) -> None:
    Pickler(file, protocol=protocol, buffer_callback=buffer_callback).dump(obj)


def dumps(
    obj: Any,
    protocol: Optional[int] = None,
    buffer_callback: Optional[Callable[[pickle.PickleBuffer], Any]] = None,
) -> bytes:
    with io.BytesIO() as file:
        Pickler(file, protocol=protocol, buffer_callback=buffer_callback).dump(obj)
        return file.getvalue()


def __getattr__(name: str) -> Any:
    # The other attributes that the bundled copy exposed, e.g., DEFAULT_PROTOCOL.
    return getattr(cloudpickle, name)

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

"""Handlers for the Arrow/Pandas UDF eval types.

Each eval type is an ``EvalTypeHandler`` subclass that declares its ``eval_type``
and self-registers in ``EVAL_TYPE_HANDLERS`` via ``__init_subclass__``; the
worker's ``read_udfs`` dispatches on the registry. Base classes live in
``_base``; concrete handlers live in private per-family submodules (``_arrow``),
imported below so importing this package registers them.
"""

# Re-export the concrete handlers from their private submodules so callers reach
# them from this package (or via EVAL_TYPE_HANDLERS), never from the ``_``
# submodules. The import also registers them.
from pyspark.sql.eval_handlers._arrow import ArrowScalarUDFHandler
from pyspark.sql.eval_handlers._base import (
    EVAL_TYPE_HANDLERS,
    BatchEvalTypeHandler,
    CoGroupedEvalTypeHandler,
    EvalTypeHandler,
    GroupedEvalTypeHandler,
)

__all__ = [
    "EVAL_TYPE_HANDLERS",
    "EvalTypeHandler",
    "BatchEvalTypeHandler",
    "GroupedEvalTypeHandler",
    "CoGroupedEvalTypeHandler",
    "ArrowScalarUDFHandler",
]

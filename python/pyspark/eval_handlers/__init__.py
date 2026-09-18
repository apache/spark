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

"""Internal handlers for the Arrow/Pandas UDF eval types.

Not a supported extension API: the registry is per executor process and filled at
import time, so handlers must be defined in this package rather than by user code.
Each eval type handled here is an ``EvalTypeHandler`` subclass (in ``_base``) that
declares its ``eval_type`` and self-registers at class definition, which
``read_udfs`` looks up via ``get_eval_type_handler``. Importing this package
imports the concrete handler submodules (``_arrow``) so they register.
"""

from pyspark.eval_handlers import _arrow  # noqa: F401  # registers handlers on import

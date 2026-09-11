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

"""Handlers for the pandas UDF eval types (the UDF exchanges ``pd.Series`` /
``pd.DataFrame`` values, converted to and from Arrow at the boundary).

No handler has been migrated here yet; the pandas eval types are still served by
the worker's ``read_udfs`` if/elif chain and will move into this module one at a
time. A concrete handler subclasses one of the category bases in the package
``__init__`` (``BatchEvalTypeHandler`` / ``GroupedEvalTypeHandler`` /
``CoGroupedEvalTypeHandler``) and declares its ``eval_type`` to self-register.
"""

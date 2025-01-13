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

from enum import Enum

# This file places the utilities for transformWithStateInPandas; we have a separate file to avoid
# putting internal classes to the stateful_processor.py file which contains public APIs.


class TransformWithStateInPandasFuncMode(Enum):
    PROCESS_DATA = 1
    PROCESS_TIMER = 2
    COMPLETE = 3
    PRE_INIT = 4

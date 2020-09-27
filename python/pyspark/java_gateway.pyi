#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.serializers import UTF8Deserializer as UTF8Deserializer, read_int as read_int, write_with_length as write_with_length  # type: ignore[attr-defined]
from typing import Any, Optional

def launch_gateway(conf: Optional[Any] = ..., popen_kwargs: Optional[Any] = ...): ...
def local_connect_and_auth(port: Any, auth_secret: Any): ...
def ensure_callback_server_started(gw: Any) -> None: ...

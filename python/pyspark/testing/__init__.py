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
import typing

from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual


grpc_requirement_message = None
try:
    import grpc
except ImportError as e:
    grpc_requirement_message = str(e)
have_grpc = grpc_requirement_message is None


grpc_status_requirement_message = None
try:
    import grpc_status
except ImportError as e:
    grpc_status_requirement_message = str(e)
have_grpc_status = grpc_status_requirement_message is None

googleapis_common_protos_requirement_message = None
try:
    from google.rpc import error_details_pb2
except ImportError as e:
    googleapis_common_protos_requirement_message = str(e)
have_googleapis_common_protos = googleapis_common_protos_requirement_message is None

graphviz_requirement_message = None
try:
    import graphviz
except ImportError as e:
    graphviz_requirement_message = str(e)
have_graphviz: bool = graphviz_requirement_message is None

from pyspark.testing.utils import PySparkErrorTestUtils
from pyspark.testing.sqlutils import pandas_requirement_message, pyarrow_requirement_message


connect_requirement_message = (
    pandas_requirement_message
    or pyarrow_requirement_message
    or grpc_requirement_message
    or googleapis_common_protos_requirement_message
    or grpc_status_requirement_message
)
should_test_connect: str = typing.cast(str, connect_requirement_message is None)

__all__ = ["assertDataFrameEqual", "assertSchemaEqual"]

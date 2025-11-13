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
from contextlib import contextmanager
from typing import Generator, Optional
from pyspark.sql import SparkSession

from typing import Any, cast


@contextmanager
def add_pipeline_analysis_context(
    spark: SparkSession, dataflow_graph_id: str, flow_name_opt: Optional[str]
) -> Generator[None, None, None]:
    """
    Context manager that add PipelineAnalysisContext extension to the user context
    used for pipeline specific analysis.
    """
    _extension_id = None
    _client = cast(Any, spark).client
    try:
        import pyspark.sql.connect.proto as pb2
        from google.protobuf import any_pb2

        _analysis_context = pb2.PipelineAnalysisContext(dataflow_graph_id=dataflow_graph_id)
        if flow_name_opt is not None:
            _analysis_context.flow_name = flow_name_opt

        _extension = any_pb2.Any()
        _extension.Pack(_analysis_context)

        _extension_id = _client.add_threadlocal_user_context_extension(_extension)
        yield
    finally:
        _client.remove_user_context_extension(_extension_id)

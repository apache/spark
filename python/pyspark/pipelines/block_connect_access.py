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
from typing import Any, Callable, Generator

from pyspark.errors import PySparkException
from pyspark.sql.connect.proto.base_pb2_grpc import SparkConnectServiceStub


BLOCKED_RPC_NAMES = ["AnalyzePlan", "ExecutePlan"]


def _is_sql_command_request(rpc_name: str, args: tuple) -> bool:
    """
    Check if the RPC call is a spark.sql() command (ExecutePlan with sql_command).

    :param rpc_name: Name of the RPC being called
    :param args: Arguments passed to the RPC
    :return: True if this is an ExecutePlan request with a sql_command
    """
    if rpc_name != "ExecutePlan" or len(args) == 0:
        return False

    request = args[0]
    if not hasattr(request, "plan"):
        return False
    plan = request.plan
    if not plan.HasField("command"):
        return False
    command = plan.command
    return command.HasField("sql_command")


@contextmanager
def block_spark_connect_execution_and_analysis() -> Generator[None, None, None]:
    """
    A context manager that blocks execution and analysis RPCs to the Spark Connect backend
    by intercepting method calls on SparkConnectServiceStub instances.

    :param error_message : Custom error message to display when communication is blocked.
        If not provided, a default message will be used.
    """
    # Store the original __getattribute__ method
    original_getattr = getattr(SparkConnectServiceStub, "__getattribute__")

    # Define a new __getattribute__ method that blocks RPC calls
    def blocked_getattr(self: SparkConnectServiceStub, name: str) -> Callable:
        original_method = original_getattr(self, name)

        def intercepted_method(*args: object, **kwargs: object) -> Any:
            # Allow all RPCs that are not AnalyzePlan or ExecutePlan
            if name not in BLOCKED_RPC_NAMES:
                return original_method(*args, **kwargs)
            # Allow spark.sql() commands (ExecutePlan with sql_command)
            elif _is_sql_command_request(name, args):
                return original_method(*args, **kwargs)
            # Block all other AnalyzePlan and ExecutePlan calls
            else:
                raise PySparkException(
                    errorClass="ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION",
                    messageParameters={},
                )

        return intercepted_method

    try:
        # Apply our custom __getattribute__ method
        setattr(SparkConnectServiceStub, "__getattribute__", blocked_getattr)
        yield
    finally:
        # Restore the original __getattribute__ method
        setattr(SparkConnectServiceStub, "__getattribute__", original_getattr)

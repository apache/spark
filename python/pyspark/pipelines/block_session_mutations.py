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
from typing import Generator, NoReturn, List, Callable

from pyspark.errors import PySparkException
from pyspark.sql.connect.catalog import Catalog
from pyspark.sql.connect.conf import RuntimeConf
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.udf import UDFRegistration

# pyspark methods that should be blocked from executing in python pipeline definition files
ERROR_CLASS = "SESSION_MUTATION_IN_DECLARATIVE_PIPELINE"
BLOCKED_METHODS: List = [
    {
        "class": RuntimeConf,
        "method": "set",
        "error_sub_class": "SET_RUNTIME_CONF",
    },
    {
        "class": Catalog,
        "method": "setCurrentCatalog",
        "error_sub_class": "SET_CURRENT_CATALOG",
    },
    {
        "class": Catalog,
        "method": "setCurrentDatabase",
        "error_sub_class": "SET_CURRENT_DATABASE",
    },
    {
        "class": Catalog,
        "method": "dropTempView",
        "error_sub_class": "DROP_TEMP_VIEW",
    },
    {
        "class": Catalog,
        "method": "dropGlobalTempView",
        "error_sub_class": "DROP_GLOBAL_TEMP_VIEW",
    },
    {
        "class": DataFrame,
        "method": "createTempView",
        "error_sub_class": "CREATE_TEMP_VIEW",
    },
    {
        "class": DataFrame,
        "method": "createOrReplaceTempView",
        "error_sub_class": "CREATE_OR_REPLACE_TEMP_VIEW",
    },
    {
        "class": DataFrame,
        "method": "createGlobalTempView",
        "error_sub_class": "CREATE_GLOBAL_TEMP_VIEW",
    },
    {
        "class": DataFrame,
        "method": "createOrReplaceGlobalTempView",
        "error_sub_class": "CREATE_OR_REPLACE_GLOBAL_TEMP_VIEW",
    },
    {
        "class": UDFRegistration,
        "method": "register",
        "error_sub_class": "REGISTER_UDF",
    },
    {
        "class": UDFRegistration,
        "method": "registerJavaFunction",
        "error_sub_class": "REGISTER_JAVA_UDF",
    },
    {
        "class": UDFRegistration,
        "method": "registerJavaUDAF",
        "error_sub_class": "REGISTER_JAVA_UDAF",
    },
]


def _create_blocked_method(error_method_name: str, error_sub_class: str) -> Callable:
    def blocked_method(*args: object, **kwargs: object) -> NoReturn:
        raise PySparkException(
            errorClass=f"{ERROR_CLASS}.{error_sub_class}",
            messageParameters={
                "method": error_method_name,
            },
        )

    return blocked_method


@contextmanager
def block_session_mutations() -> Generator[None, None, None]:
    """
    Context manager that blocks imperative constructs found in a pipeline python definition file
    See BLOCKED_METHODS above for a list
    """
    # Store original methods
    original_methods = {}
    for method_info in BLOCKED_METHODS:
        cls = method_info["class"]
        method_name = method_info["method"]
        original_methods[(cls, method_name)] = getattr(cls, method_name)

    try:
        # Replace methods with blocked versions
        for method_info in BLOCKED_METHODS:
            cls = method_info["class"]
            method_name = method_info["method"]
            error_method_name = f"'{cls.__name__}.{method_name}'"
            blocked_method = _create_blocked_method(
                error_method_name, method_info["error_sub_class"]
            )
            setattr(cls, method_name, blocked_method)

        yield
    finally:
        # Restore original methods
        for method_info in BLOCKED_METHODS:
            cls = method_info["class"]
            method_name = method_info["method"]
            original_method = original_methods[(cls, method_name)]
            setattr(cls, method_name, original_method)

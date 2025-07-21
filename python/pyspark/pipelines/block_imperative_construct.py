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
from typing import Generator, NoReturn, Union

from pyspark.errors import PySparkException
from pyspark.sql.connect.conf import RuntimeConf
from pyspark.sql.connect.catalog import Catalog
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.udf import UDFRegistration

# pyspark methods that should be blocked from executing in user supplied python pipeline definition files
BLOCKED_METHODS = [
    {
        "class": RuntimeConf,
        "method": "set",
        "suggestion": "Instead set configuration via the pipeline spec or use the 'spark_conf' argument in various decorators"
    },
    {
        "class": Catalog,
        "method": "setCurrentCatalog",
        "suggestion": "Instead set catalog via the pipeline spec or the 'name' argument on the dataset decorators"
    },
    {
        "class": Catalog,
        "method": "setCurrentDatabase",
        "suggestion": "Instead set database via the pipeline spec or the 'name' argument on the dataset decorators"
    },
    {
        "class": Catalog,
        "method": "dropTempView",
        "suggestion": "Instead remove the temporary view definition directly"

    },
    {
        "class": Catalog,
        "method": "dropGlobalTempView",
        "suggestion": "Instead remove the temporary view definition directly"
    },
    {
        "class": DataFrame,
        "method": "createTempView",
        "suggestion": "Instead use the @temporary_view decorator to define temporary views"
    },
    {
        "class": DataFrame,
        "method": "createOrReplaceTempView",
        "suggestion": "Instead use the @temporary_view decorator to define temporary views"
    },
    {
        "class": DataFrame,
        "method": "createGlobalTempView",
        "suggestion": "Instead use the @temporary_view decorator to define temporary views"
    },
    {
        "class": DataFrame,
        "method": "createOrReplaceGlobalTempView",
        "suggestion": "Instead use the @temporary_view decorator to define temporary views"
    },
    {
        "class": UDFRegistration,
        "method": "register",
        "suggestion": "",
    },
    {
        "class": UDFRegistration,
        "method": "registerJavaFunction",
        "suggestion": "",
    },
    {
        "class": UDFRegistration,
        "method": "registerJavaUDAF",
        "suggestion": "",
    },
]

def _create_blocked_method(error_method_name: str, suggestion: str):
    def blocked_method(*args, **kwargs) -> NoReturn:
        raise PySparkException(
            errorClass="IMPERATIVE_CONSTRUCT_IN_DECLARATIVE_PIPELINE",
            messageParameters={
                "method": error_method_name,
                "suggestion": suggestion,
            },
        )
    return blocked_method


@contextmanager
def block_imperative_construct() -> Generator[None, None, None]:
    """
    Context manager that blocks imperative constructs found in a pipeline python definition file
    Blocks:
        - imperative config set via: spark.conf.set("k", "v")
        - catalog changes via: spark.catalog.setCurrentCatalog("catalog_name")
        - database changes via: spark.catalog.setCurrentDatabase("db_name")
        - temporary view creation/deletion via DataFrame and catalog methods
        - user-defined functions registration
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
            blocked_method = _create_blocked_method(error_method_name, method_info["suggestion"])
            setattr(cls, method_name, blocked_method)

        yield
    finally:
        # Restore original methods
        for method_info in BLOCKED_METHODS:
            cls = method_info["class"]
            method_name = method_info["method"]
            original_method = original_methods[(cls, method_name)]
            setattr(cls, method_name, original_method)

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


@contextmanager
def block_imperative_construct() -> Generator[None, None, None]:
    """
    Context manager that blocks imperative constructs found in a pipeline python definition file
    Blocks:
        - imperative config set via: spark.conf.set("k", "v")
        - catalog changes via: spark.catalog.setCurrentCatalog("catalog_name")
        - database changes via: spark.catalog.setCurrentDatabase("db_name")
    """
    # store the original methods
    original_connect_set = RuntimeConf.set
    original_connect_catalog_set_current_catalog = Catalog.setCurrentCatalog
    original_connect_catalog_set_current_database = Catalog.setCurrentDatabase

    def blocked_conf_set(self: RuntimeConf, key: str, value: Union[str, int, bool]) -> NoReturn:
        raise PySparkException(
            errorClass="IMPERATIVE_CONF_SET_IN_DECLARATIVE_PIPELINE",
            messageParameters={
                "method": "'spark.conf.set'",
            },
        )

    def blocked_connect_catalog_set_current_catalog(self: Catalog, catalogName: str) -> NoReturn:
        raise PySparkException(
            errorClass="IMPERATIVE_CONF_SET_IN_DECLARATIVE_PIPELINE",
            messageParameters={
                "method": "'spark.catalog.setCurrentCatalog'",
            },
        )

    def blocked_connect_catalog_set_current_database(self: Catalog, dbName: str) -> NoReturn:
        raise PySparkException(
            errorClass="IMPERATIVE_CONF_SET_IN_DECLARATIVE_PIPELINE",
            messageParameters={
                "method": "'spark.catalog.setCurrentDatabase'",
            },
        )

    try:
        setattr(RuntimeConf, "set", blocked_conf_set)
        setattr(Catalog, "setCurrentCatalog", blocked_connect_catalog_set_current_catalog)
        setattr(Catalog, "setCurrentDatabase", blocked_connect_catalog_set_current_database)
        yield
    finally:
        setattr(RuntimeConf, "set", original_connect_set)
        setattr(Catalog, "setCurrentCatalog", original_connect_catalog_set_current_catalog)
        setattr(Catalog, "setCurrentDatabase", original_connect_catalog_set_current_database)

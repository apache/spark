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
from pyspark.sql.connect.utils import check_dependencies

check_dependencies(__name__)


from typing import TYPE_CHECKING

import pyspark.sql.connect.proto as pb2
from pyspark.storagelevel import StorageLevel

if TYPE_CHECKING:
    from pyspark.sql.connect.dataframe import DataFrame


def storage_level_to_proto(storage_level: StorageLevel) -> pb2.StorageLevel:
    assert storage_level is not None and isinstance(storage_level, StorageLevel)
    return pb2.StorageLevel(
        use_disk=storage_level.useDisk,
        use_memory=storage_level.useMemory,
        use_off_heap=storage_level.useOffHeap,
        deserialized=storage_level.deserialized,
        replication=storage_level.replication,
    )


def proto_to_storage_level(storage_level: pb2.StorageLevel) -> StorageLevel:
    assert storage_level is not None and isinstance(storage_level, pb2.StorageLevel)
    return StorageLevel(
        useDisk=storage_level.use_disk,
        useMemory=storage_level.use_memory,
        useOffHeap=storage_level.use_off_heap,
        deserialized=storage_level.deserialized,
        replication=storage_level.replication,
    )


def proto_to_remote_cached_dataframe(relation: pb2.CachedRemoteRelation) -> "DataFrame":
    assert relation is not None and isinstance(relation, pb2.CachedRemoteRelation)

    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.session import SparkSession
    import pyspark.sql.connect.plan as plan

    session = SparkSession.active()
    return DataFrame(
        plan=plan.CachedRemoteRelation(relation.relation_id, session),
        session=session,
    )

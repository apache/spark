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

from typing import ClassVar

class StorageLevel:
    DISK_ONLY: ClassVar[StorageLevel]
    DISK_ONLY_2: ClassVar[StorageLevel]
    MEMORY_ONLY: ClassVar[StorageLevel]
    MEMORY_ONLY_2: ClassVar[StorageLevel]
    DISK_ONLY_3: ClassVar[StorageLevel]
    MEMORY_AND_DISK: ClassVar[StorageLevel]
    MEMORY_AND_DISK_2: ClassVar[StorageLevel]
    OFF_HEAP: ClassVar[StorageLevel]

    useDisk: bool
    useMemory: bool
    useOffHeap: bool
    deserialized: bool
    replication: int
    def __init__(
        self,
        useDisk: bool,
        useMemory: bool,
        useOffHeap: bool,
        deserialized: bool,
        replication: int = ...,
    ) -> None: ...

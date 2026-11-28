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

__all__ = ["StorageLevel"]

from typing import Any, ClassVar


class StorageLevel:
    """
    Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
    whether to drop the RDD to disk if it falls out of memory, whether to keep the data in memory
    in a JAVA-specific serialized format, and whether to replicate the RDD partitions on multiple
    nodes. Also contains static constants for some commonly used storage levels, MEMORY_ONLY.
    Since the data is always serialized on the Python side, all the constants use the serialized
    formats.
    """

    NONE: ClassVar["StorageLevel"]
    DISK_ONLY: ClassVar["StorageLevel"]
    DISK_ONLY_2: ClassVar["StorageLevel"]
    DISK_ONLY_3: ClassVar["StorageLevel"]
    MEMORY_ONLY: ClassVar["StorageLevel"]
    MEMORY_ONLY_2: ClassVar["StorageLevel"]
    MEMORY_AND_DISK: ClassVar["StorageLevel"]
    MEMORY_AND_DISK_2: ClassVar["StorageLevel"]
    OFF_HEAP: ClassVar["StorageLevel"]
    MEMORY_AND_DISK_DESER: ClassVar["StorageLevel"]

    def __init__(
        self,
        useDisk: bool,
        useMemory: bool,
        useOffHeap: bool,
        deserialized: bool,
        replication: int = 1,
    ):
        self.useDisk = useDisk
        self.useMemory = useMemory
        self.useOffHeap = useOffHeap
        self.deserialized = deserialized
        self.replication = replication

    @staticmethod
    def fromString(name: str) -> "StorageLevel":
        """
        Create a storage level from its Scala name, including ``NONE``.

        .. versionadded:: 4.5.0

        Parameters
        ----------
        name : str
            Case-sensitive storage level name accepted by Scala's ``StorageLevel.fromString``.

        Returns
        -------
        :class:`StorageLevel`
            Storage level with the corresponding memory, disk, serialization, and replication flags.

        Notes
        -----
        Memory-based Scala levels without ``_SER`` use deserialized storage. ``MEMORY_AND_DISK``
        corresponds to :attr:`StorageLevel.MEMORY_AND_DISK_DESER`, while ``MEMORY_AND_DISK_SER``
        corresponds to :attr:`StorageLevel.MEMORY_AND_DISK`.

        Examples
        --------
        >>> StorageLevel.fromString("DISK_ONLY") == StorageLevel.DISK_ONLY
        True
        >>> StorageLevel.fromString("MEMORY_AND_DISK") == StorageLevel.MEMORY_AND_DISK_DESER
        True
        >>> StorageLevel.fromString("NONE") == StorageLevel.NONE
        True
        """
        from pyspark.errors import PySparkValueError

        levels = {
            "NONE": StorageLevel.NONE,
            "DISK_ONLY": StorageLevel.DISK_ONLY,
            "DISK_ONLY_2": StorageLevel.DISK_ONLY_2,
            "DISK_ONLY_3": StorageLevel.DISK_ONLY_3,
            "MEMORY_ONLY": StorageLevel(False, True, False, True),
            "MEMORY_ONLY_2": StorageLevel(False, True, False, True, 2),
            "MEMORY_ONLY_SER": StorageLevel.MEMORY_ONLY,
            "MEMORY_ONLY_SER_2": StorageLevel.MEMORY_ONLY_2,
            "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK_DESER,
            "MEMORY_AND_DISK_2": StorageLevel(True, True, False, True, 2),
            "MEMORY_AND_DISK_SER": StorageLevel.MEMORY_AND_DISK,
            "MEMORY_AND_DISK_SER_2": StorageLevel.MEMORY_AND_DISK_2,
            "OFF_HEAP": StorageLevel.OFF_HEAP,
        }
        if name not in levels:
            raise PySparkValueError(
                errorClass="VALUE_NOT_ALLOWED",
                messageParameters={"arg_name": "name", "allowed_values": str(list(levels))},
            )
        return levels[name]

    def __repr__(self) -> str:
        return "StorageLevel(%s, %s, %s, %s, %s)" % (
            self.useDisk,
            self.useMemory,
            self.useOffHeap,
            self.deserialized,
            self.replication,
        )

    def __str__(self) -> str:
        result = ""
        result += "Disk " if self.useDisk else ""
        result += "Memory " if self.useMemory else ""
        result += "OffHeap " if self.useOffHeap else ""
        result += "Deserialized " if self.deserialized else "Serialized "
        result += "%sx Replicated" % self.replication
        return result

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, StorageLevel)
            and self.useMemory == other.useMemory
            and self.useDisk == other.useDisk
            and self.useOffHeap == other.useOffHeap
            and self.deserialized == other.deserialized
            and self.replication == other.replication
        )


StorageLevel.NONE = StorageLevel(False, False, False, False)
StorageLevel.DISK_ONLY = StorageLevel(True, False, False, False)
StorageLevel.DISK_ONLY_2 = StorageLevel(True, False, False, False, 2)
StorageLevel.DISK_ONLY_3 = StorageLevel(True, False, False, False, 3)
StorageLevel.MEMORY_ONLY = StorageLevel(False, True, False, False)
StorageLevel.MEMORY_ONLY_2 = StorageLevel(False, True, False, False, 2)
StorageLevel.MEMORY_AND_DISK = StorageLevel(True, True, False, False)
StorageLevel.MEMORY_AND_DISK_2 = StorageLevel(True, True, False, False, 2)
StorageLevel.OFF_HEAP = StorageLevel(True, True, True, False, 1)
StorageLevel.MEMORY_AND_DISK_DESER = StorageLevel(True, True, False, True)

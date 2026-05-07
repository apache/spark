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

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


# Class for maintaining information about a spatial reference system (SRS).
@dataclass(frozen=True)
class SpatialReferenceSystemInformation:
    # Field storing the spatial reference identifier (SRID) value of this SRS.
    srid: int
    # Field storing the string ID of the corresponding coordinate reference system (CRS).
    string_id: str
    # Field indicating whether the spatial reference system (SRS) is geographic or not.
    is_geographic: bool


# Class for maintaining the mappings between supported SRID/CRS values and the corresponding SRS.
class SpatialReferenceSystemCache:
    # We use a singleton pattern, which is aligned with the JVM side / Scala implementation.
    _instance: Optional["SpatialReferenceSystemCache"] = None

    @classmethod
    def instance(cls) -> "SpatialReferenceSystemCache":
        if cls._instance is None:
            cls._instance = SpatialReferenceSystemCache()
        return cls._instance

    def __init__(self) -> None:
        # Hash map for defining the mappings from the integer SRID to the full SRS information.
        self._srid_to_srs: Dict[int, SpatialReferenceSystemInformation] = {}
        # Hash map for defining the mappings from the string ID to the full SRS information.
        self._string_id_to_srs: Dict[str, SpatialReferenceSystemInformation] = {}
        self._populate_srs_information_mapping()

    # Helper method for building the SRID-to-SRS and stringID-to-SRS mappings.
    def _populate_srs_information_mapping(self) -> None:
        # Currently, we only support a limited set of SRID / CRS values, even on Scala side. The
        # SRS list below will be updated soon, and the maps will be populated with more SRS data.
        srs_list = [
            SpatialReferenceSystemInformation(0, "SRID:0", False),
            SpatialReferenceSystemInformation(3857, "EPSG:3857", False),
            SpatialReferenceSystemInformation(4326, "OGC:CRS84", True),
        ]
        # Populate the mappings using the same SRS information objects, avoiding any duplication.
        for srs in srs_list:
            self._srid_to_srs[srs.srid] = srs
            self._string_id_to_srs[srs.string_id] = srs

    # Returns the SRS corresponding to the input SRID. If not supported, returns `None`.
    def get_srs_by_srid(self, srid: int) -> Optional[SpatialReferenceSystemInformation]:
        return self._srid_to_srs.get(srid)

    # Returns the SRS corresponding to the input string ID. If not supported, returns `None`.
    def get_srs_by_string_id(self, string_id: str) -> Optional[SpatialReferenceSystemInformation]:
        return self._string_id_to_srs.get(string_id)


# Class for providing SRS mappings for geographic spatial reference systems.
class GeographicSpatialReferenceSystemMapper:
    # Returns the string ID corresponding to the input SRID. If not supported, returns `None`.
    @staticmethod
    def get_string_id(srid: int) -> Optional[str]:
        srs = SpatialReferenceSystemCache.instance().get_srs_by_srid(srid)
        return srs.string_id if srs is not None and srs.is_geographic else None

    # Returns the SRID corresponding to the input string ID. If not supported, returns `None`.
    @staticmethod
    def get_srid(string_id: str) -> Optional[int]:
        srs = SpatialReferenceSystemCache.instance().get_srs_by_string_id(string_id)
        return srs.srid if srs is not None and srs.is_geographic else None


# Class for providing SRS mappings for cartesian spatial reference systems.
class CartesianSpatialReferenceSystemMapper:
    # Returns the string ID corresponding to the input SRID. If not supported, returns `None`.
    @staticmethod
    def get_string_id(srid: int) -> Optional[str]:
        srs = SpatialReferenceSystemCache.instance().get_srs_by_srid(srid)
        return srs.string_id if srs is not None else None

    # Returns the SRID corresponding to the input string ID. If not supported, returns `None`.
    @staticmethod
    def get_srid(string_id: str) -> Optional[int]:
        srs = SpatialReferenceSystemCache.instance().get_srs_by_string_id(string_id)
        return srs.srid if srs is not None else None

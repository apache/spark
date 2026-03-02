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

import csv
import os
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
        self._load_srs_registry_csv()
        self._add_spark_specific_entries()

    def _load_srs_registry_csv(self) -> None:
        """Load SRS entries from the CSV resource file generated from the PROJ EPSG database."""
        csv_path = os.path.join(os.path.dirname(__file__), "srs_registry.csv")
        with open(csv_path, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or row[0].startswith("#") or row[0] == "srid":
                    continue
                srid = int(row[0])
                string_id = row[1]
                is_geographic = row[2].strip().lower() == "true"
                srs = SpatialReferenceSystemInformation(srid, string_id, is_geographic)
                self._srid_to_srs[srid] = srs
                self._string_id_to_srs[string_id] = srs

    def _add_spark_specific_entries(self) -> None:
        """Add Spark-specific SRS entries and aliases for storage format compatibility."""
        # SRID 0: Cartesian coordinate system with no defined SRS (Spark convention).
        srid0 = SpatialReferenceSystemInformation(0, "SRID:0", False)
        self._srid_to_srs[0] = srid0
        self._string_id_to_srs["SRID:0"] = srid0

        # SRIDs 4326, 4267, and 4269 are standardized under OGC rather than EPSG.
        # Override their primary string IDs and keep the EPSG aliases.
        # OGC:CRS84 is also used by Parquet, Delta, and Iceberg for SRID 4326.
        self._add_ogc_override(4326, "OGC:CRS84")
        self._add_ogc_override(4267, "OGC:CRS27")
        self._add_ogc_override(4269, "OGC:CRS83")

    def _add_ogc_override(self, srid: int, ogc_string_id: str) -> None:
        """Override a PROJ EPSG entry with an OGC string ID, keeping the EPSG alias."""
        existing = self._srid_to_srs.get(srid)
        if existing is not None:
            ogc_entry = SpatialReferenceSystemInformation(
                srid, ogc_string_id, existing.is_geographic
            )
            self._srid_to_srs[srid] = ogc_entry
            self._string_id_to_srs[ogc_string_id] = ogc_entry
            self._string_id_to_srs[existing.string_id] = ogc_entry

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

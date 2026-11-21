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

import unittest

from pyspark.sql.geo_utils import (
    CartesianSpatialReferenceSystemMapper,
    GeographicSpatialReferenceSystemMapper,
    SpatialReferenceSystemCache,
    SpatialReferenceSystemInformation,
)


# The Python counterpart of SpatialReferenceSystemMapperSuite on the JVM side: the mappers read
# the same registry, so the same SRIDs and CRS string IDs must resolve, and nothing else.
class SpatialReferenceSystemMapperTests(unittest.TestCase):
    def test_spatial_reference_system_information(self):
        for srid, string_id, is_geographic in [
            (0, "SRID:0", False),
            (3857, "EPSG:3857", False),
            (4326, "OGC:CRS84", True),
        ]:
            srs = SpatialReferenceSystemInformation(srid, string_id, is_geographic)
            self.assertEqual(srs.srid, srid)
            self.assertEqual(srs.string_id, string_id)
            self.assertEqual(srs.is_geographic, is_geographic)

    def test_get_string_id_returns_correct_string_id_for_valid_srid(self):
        cartesian = CartesianSpatialReferenceSystemMapper.get_string_id
        self.assertEqual(cartesian(0), "SRID:0")
        self.assertEqual(cartesian(3857), "EPSG:3857")
        self.assertEqual(cartesian(4326), "OGC:CRS84")
        self.assertEqual(cartesian(4267), "OGC:CRS27")
        self.assertEqual(cartesian(4269), "OGC:CRS83")
        self.assertEqual(cartesian(32601), "EPSG:32601")
        self.assertEqual(cartesian(102013), "ESRI:102013")
        self.assertEqual(cartesian(102964), "ESRI:102964")
        self.assertEqual(cartesian(105604), "ESRI:105604")
        geographic = GeographicSpatialReferenceSystemMapper.get_string_id
        self.assertEqual(geographic(4326), "OGC:CRS84")
        self.assertEqual(geographic(4267), "OGC:CRS27")
        self.assertEqual(geographic(4269), "OGC:CRS83")
        self.assertEqual(geographic(4612), "EPSG:4612")
        self.assertEqual(geographic(37001), "ESRI:37001")
        self.assertEqual(geographic(104030), "ESRI:104030")

    def test_get_string_id_returns_none_for_invalid_srid(self):
        cartesian = CartesianSpatialReferenceSystemMapper.get_string_id
        for srid in [-2, -1, 1, 2, 9999, 999999]:
            self.assertIsNone(cartesian(srid), srid)
        geographic = GeographicSpatialReferenceSystemMapper.get_string_id
        for srid in [-2, -1, 0, 1, 2, 3857, 9999, 999999]:
            self.assertIsNone(geographic(srid), srid)

    def test_get_srid_returns_correct_srid_for_valid_string_id(self):
        cartesian = CartesianSpatialReferenceSystemMapper.get_srid
        self.assertEqual(cartesian("SRID:0"), 0)
        self.assertEqual(cartesian("EPSG:3857"), 3857)
        self.assertEqual(cartesian("OGC:CRS84"), 4326)
        self.assertEqual(cartesian("OGC:CRS27"), 4267)
        self.assertEqual(cartesian("OGC:CRS83"), 4269)
        self.assertEqual(cartesian("EPSG:4326"), 4326)
        self.assertEqual(cartesian("EPSG:4267"), 4267)
        self.assertEqual(cartesian("EPSG:4269"), 4269)
        self.assertEqual(cartesian("EPSG:32601"), 32601)
        self.assertEqual(cartesian("ESRI:102964"), 102964)
        self.assertEqual(cartesian("ESRI:105604"), 105604)
        geographic = GeographicSpatialReferenceSystemMapper.get_srid
        self.assertEqual(geographic("OGC:CRS84"), 4326)
        self.assertEqual(geographic("ESRI:104030"), 104030)

    def test_get_srid_returns_none_for_invalid_string_id(self):
        cartesian = CartesianSpatialReferenceSystemMapper.get_srid
        for string_id in ["INVALID:ID", "EPSG:999999"]:
            self.assertIsNone(cartesian(string_id), string_id)
        geographic = GeographicSpatialReferenceSystemMapper.get_srid
        for string_id in ["INVALID:ID", "EPSG:999999", "SRID:0", "EPSG:3857"]:
            self.assertIsNone(geographic(string_id), string_id)

    def test_map_validity(self):
        cache = SpatialReferenceSystemCache.instance()
        srid_to_srs = cache.srid_to_srs
        string_id_to_srs = cache.string_id_to_srs
        for srid, by_srid in srid_to_srs.items():
            self.assertEqual(by_srid.srid, srid)
            # The canonical string ID of every SRID resolves to the very same SRS.
            self.assertIs(string_id_to_srs.get(by_srid.string_id), by_srid, srid)
        for string_id, by_string_id in string_id_to_srs.items():
            # Every string ID, canonical or an EPSG alias of an OGC override, names a known SRID.
            self.assertIs(srid_to_srs.get(by_string_id.srid), by_string_id, string_id)

    def test_spatial_reference_system_contents(self):
        srid_to_srs = SpatialReferenceSystemCache.instance().srid_to_srs
        by_authority = {"SRID:0": 0, "OGC:": 0, "EPSG:": 0, "ESRI:": 0}
        for srs in srid_to_srs.values():
            for prefix in by_authority:
                if srs.string_id == prefix or srs.string_id.startswith(prefix):
                    by_authority[prefix] += 1
                    break
        # The same counts as the JVM side's SpatialReferenceSystemMapperSuite.
        self.assertEqual(by_authority["SRID:0"], 1)
        self.assertEqual(by_authority["OGC:"], 3)
        self.assertEqual(by_authority["EPSG:"], 7720)
        self.assertEqual(by_authority["ESRI:"], 2990)
        self.assertEqual(len(srid_to_srs), 10714)


if __name__ == "__main__":
    from pyspark.testing import main

    main()

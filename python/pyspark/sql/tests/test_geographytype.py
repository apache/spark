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

from pyspark.sql.types import GeographyType, GeometryType
from pyspark.sql.utils import IllegalArgumentException
from pyspark.testing.sqlutils import ReusedSQLTestCase


class GeographyTypeTestMixin:
    # Test cases for GeographyType construction based on SRID.

    def test_geographytype_specified_valid_srid(self):
        """Test that GeographyType is constructed correctly when a valid SRID is specified."""

        supported_srid = {
            4326: "OGC:CRS84",
            # The OGC standardizations are the canonical string IDs of their EPSG codes.
            4267: "OGC:CRS27",
            4269: "OGC:CRS83",
            # Any geographic CRS in the PROJ registry PySpark ships, EPSG or ESRI.
            4612: "EPSG:4612",
            104030: "ESRI:104030",
        }

        for srid, crs in supported_srid.items():
            geography_type = GeographyType(srid)
            self.assertEqual(geography_type.srid, srid)
            self.assertEqual(geography_type.typeName(), "geography")
            self.assertEqual(geography_type.simpleString(), f"geography({srid})")
            self.assertEqual(geography_type.jsonValue(), f"geography({crs}, SPHERICAL)")
            self.assertEqual(repr(geography_type), f"GeographyType({srid})")

    def test_geographytype_specified_invalid_srid(self):
        """Test that the correct error is returned when an invalid SRID value is specified."""

        # A projected CRS, such as SRID 0 or Web Mercator, is not a geography either.
        for srid in [-4612, -4326, -2, -1, 0, 1, 2, 3857, 32610, 9999, 999999]:
            with self.assertRaises(IllegalArgumentException) as error_context:
                GeographyType(srid)
            srid_header = "[ST_INVALID_SRID_VALUE] Invalid or unsupported SRID"
            self.assertEqual(
                str(error_context.exception),
                f"{srid_header} (spatial reference identifier) value: {srid}.",
            )

    # Special string value "ANY" in place of SRID is used to denote a mixed GEOGRAPHY type.

    def test_geographytype_any_specifier(self):
        """Test that GeographyType is constructed correctly with ANY specifier for mixed SRID."""

        geography_type = GeographyType("ANY")
        self.assertEqual(geography_type.srid, GeographyType.MIXED_SRID)
        self.assertEqual(geography_type.typeName(), "geography")
        self.assertEqual(geography_type.simpleString(), "geography(any)")
        self.assertEqual(repr(geography_type), "GeographyType(ANY)")

    # The tests below verify GEOGRAPHY type object equality based on SRID values.

    def test_geographytype_same_srid_values(self):
        """Test that two GeographyTypes with the same SRID are equal, however they were built."""

        for srid in [4326, 4269, 4612]:
            geography_type_1 = GeographyType(srid)
            geography_type_2 = GeographyType(srid)
            self.assertEqual(geography_type_1.srid, geography_type_2.srid)
            self.assertEqual(geography_type_1, geography_type_2)
            self.assertEqual(hash(geography_type_1), hash(geography_type_2))

        # Equality depends on the SRID and the algorithm only, as on the JVM side: a type parsed
        # from any CRS string equals the type constructed from the SRID it resolves to.
        for crs, srid in {"EPSG:4326": 4326, "OGC:CRS84": 4326, "EPSG:4269": 4269}.items():
            geography_type_from_crs = GeographyType._from_crs(crs, "SPHERICAL")
            self.assertEqual(geography_type_from_crs, GeographyType(srid))
            self.assertEqual(GeographyType(srid), geography_type_from_crs)
            self.assertEqual(hash(geography_type_from_crs), hash(GeographyType(srid)))
        self.assertEqual(
            GeographyType._from_crs("EPSG:4326", "SPHERICAL"),
            GeographyType._from_crs("OGC:CRS84", "SPHERICAL"),
        )
        self.assertEqual(GeographyType("ANY"), GeographyType._from_crs("srid:any", "SPHERICAL"))
        self.assertEqual(
            len({GeographyType(4326), GeographyType._from_crs("EPSG:4326", "SPHERICAL")}), 1
        )

    def test_geographytype_different_srid_values(self):
        """Test that two GeographyTypes with different SRIDs are not equal."""

        for srid in [4326, 4269]:
            geography_type_1 = GeographyType(srid)
            geography_type_2 = GeographyType("ANY")
            self.assertNotEqual(geography_type_1.srid, geography_type_2.srid)
            self.assertNotEqual(geography_type_1, geography_type_2)
            geography_type_3 = GeographyType(4612)
            self.assertNotEqual(geography_type_1.srid, geography_type_3.srid)
            self.assertNotEqual(geography_type_1, geography_type_3)

        # A geography and a geometry with the same SRID are different types.
        self.assertNotEqual(GeographyType(4326), GeometryType(4326))
        self.assertNotEqual(GeographyType("ANY"), GeometryType("ANY"))

    def test_geographytype_from_invalid_crs(self):
        """Test that GeographyType construction fails when an invalid CRS is specified."""

        for invalid_crs in [
            "srid",
            "any",
            "ogccrs84",
            "ogc:crs84",
            "ogc:CRS84",
            "asdf",
            "",
            "SRID:0",
            "EPSG:3857",
            "EPSG:999999",
        ]:
            with self.assertRaises(IllegalArgumentException) as error_context:
                GeographyType._from_crs(invalid_crs, "SPHERICAL")
            crs_header = "[ST_INVALID_CRS_VALUE] Invalid or unsupported CRS"
            self.assertEqual(
                str(error_context.exception),
                f"{crs_header} (coordinate reference system) value: '{invalid_crs}'.",
            )

    # The tests below verify GEOGRAPHY type JSON parsing based on CRS and algorithm.

    def test_geographytype_from_invalid_algorithm(self):
        """Test that GeographyType construction fails when an invalid CRS is specified."""

        for invalid_alg in ["alg", "algorithm", "KARNEY", "spherical", "SPHEROID", "asdf", ""]:
            with self.assertRaises(IllegalArgumentException) as error_context:
                GeographyType._from_crs("OGC:CRS84", invalid_alg)
            alg_header = "[ST_INVALID_ALGORITHM_VALUE] Invalid or unsupported"
            self.assertEqual(
                str(error_context.exception),
                f"{alg_header} edge interpolation algorithm value: '{invalid_alg}'.",
            )

    def test_geographytype_from_valid_crs_and_algorithm(self):
        """Test that GeographyType construction passes when valid CRS & ALG are specified."""

        supported_crs = {
            "OGC:CRS84": 4326,
            "EPSG:4326": 4326,
            "OGC:CRS27": 4267,
            "EPSG:4612": 4612,
            "ESRI:104030": 104030,
        }
        for valid_crs, srid in supported_crs.items():
            for valid_alg in ["SPHERICAL"]:
                geography_type = GeographyType._from_crs(valid_crs, valid_alg)
                self.assertEqual(geography_type.srid, srid)
                self.assertEqual(geography_type.typeName(), "geography")
                self.assertEqual(geography_type.simpleString(), f"geography({srid})")
                self.assertEqual(geography_type.jsonValue(), f"geography({valid_crs}, {valid_alg})")
                self.assertEqual(repr(geography_type), f"GeographyType({srid})")


class GeographyTypeTest(GeographyTypeTestMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()

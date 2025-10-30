# -*- encoding: utf-8 -*-
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

from pyspark.sql.types import GeometryType
from pyspark.sql.utils import IllegalArgumentException
from pyspark.testing.sqlutils import ReusedSQLTestCase


class GeometryTypeTestMixin:
    # Test cases for GeometryType construction based on SRID.

    def test_geometrytype_specified_valid_srid(self):
        """Test that GeometryType is constructed correctly when a valid SRID is specified."""

        supported_srid = {0: "SRID:0", 3857: "EPSG:3857", 4326: "OGC:CRS84"}

        for srid, crs in supported_srid.items():
            geometry_type = GeometryType(srid)
            self.assertEqual(geometry_type.srid, srid)
            self.assertEqual(geometry_type.typeName(), "geometry")
            self.assertEqual(geometry_type.simpleString(), f"geometry({srid})")
            self.assertEqual(geometry_type.jsonValue(), f"geometry({crs})")
            self.assertEqual(repr(geometry_type), f"GeometryType({srid})")

    def test_geometrytype_specified_invalid_srid(self):
        """Test that the correct error is returned when an invalid SRID value is specified."""

        for srid in [-4612, -4326, -2, -1, 1, 2]:
            with self.assertRaises(IllegalArgumentException) as error_context:
                GeometryType(srid)
            srid_header = "[ST_INVALID_SRID_VALUE] Invalid or unsupported SRID"
            self.assertEqual(
                str(error_context.exception),
                f"{srid_header} (spatial reference identifier) value: {srid}.",
            )

    # Special string value "ANY" in place of SRID is used to denote a mixed GEOMETRY type.

    def test_geometrytype_any_specifier(self):
        """Test that GeometryType is constructed correctly with ANY specifier for mixed SRID."""

        geometry_type = GeometryType("ANY")
        self.assertEqual(geometry_type.srid, GeometryType.MIXED_SRID)
        self.assertEqual(geometry_type.typeName(), "geometry")
        self.assertEqual(geometry_type.simpleString(), "geometry(any)")
        self.assertEqual(repr(geometry_type), "GeometryType(ANY)")

    # The tests below verify GEOMETRY type object equality based on SRID values.

    def test_geometrytype_same_srid_values(self):
        """Test that two GeometryTypes with specified SRIDs have the same SRID values."""

        for srid in [0, 3857, 4326]:
            geometry_type_1 = GeometryType(srid)
            geometry_type_2 = GeometryType(srid)
            self.assertEqual(geometry_type_1.srid, geometry_type_2.srid)

    def test_geometrytype_different_srid_values(self):
        """Test that two GeometryTypes with specified SRIDs have different SRID values."""

        for srid in [0, 4326]:
            geometry_type_1 = GeometryType(srid)
            geometry_type_2 = GeometryType("ANY")
            self.assertNotEqual(geometry_type_1.srid, geometry_type_2.srid)
            geometry_type_3 = GeometryType(3857)
            self.assertNotEqual(geometry_type_1.srid, geometry_type_3.srid)

    # The tests below verify GEOMETRY type JSON parsing based on the CRS value.

    def test_geometrytype_from_invalid_crs(self):
        """Test that GeometryType construction fails when an invalid CRS is specified."""

        for invalid_crs in ["srid", "any", "ogccrs84", "ogc:crs84", "ogc:CRS84", "asdf", ""]:
            with self.assertRaises(IllegalArgumentException) as error_context:
                GeometryType._from_crs(invalid_crs)
            crs_header = "[ST_INVALID_CRS_VALUE] Invalid or unsupported CRS"
            self.assertEqual(
                str(error_context.exception),
                f"{crs_header} (coordinate reference system) value: '{invalid_crs}'.",
            )

    def test_geometrytype_from_valid_crs(self):
        """Test that GeometryType construction passes when a valid CRS is specified."""

        supported_crs = {
            "SRID:0": 0,
            "EPSG:3857": 3857,
            "OGC:CRS84": 4326,
        }
        for valid_crs, srid in supported_crs.items():
            geometry_type = GeometryType._from_crs(valid_crs)
            self.assertEqual(geometry_type.srid, srid)
            self.assertEqual(geometry_type.typeName(), "geometry")
            self.assertEqual(geometry_type.simpleString(), f"geometry({srid})")
            self.assertEqual(geometry_type.jsonValue(), f"geometry({valid_crs})")
            self.assertEqual(repr(geometry_type), f"GeometryType({srid})")


class GeometryTypeTest(GeometryTypeTestMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_geometrytype import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

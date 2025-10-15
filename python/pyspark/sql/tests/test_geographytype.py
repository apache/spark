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

from pyspark.sql.types import GeographyType
from pyspark.sql.utils import IllegalArgumentException
from pyspark.testing.sqlutils import ReusedSQLTestCase


class GeographyTypeTestMixin:

    # Test cases for GeographyType construction based on SRID.

    def test_geographytype_specified_valid_srid(self):
        """Test that GeographyType is constructed correctly when a valid SRID is specified."""

        supported_srid = {4326: "OGC:CRS84"}

        for srid, crs in supported_srid.items():
            geography_type = GeographyType(srid)
            self.assertEqual(geography_type.srid, srid)
            self.assertEqual(geography_type.typeName(), "geography")
            self.assertEqual(geography_type.simpleString(), f"geography({srid})")
            self.assertEqual(geography_type.jsonValue(), f"geography({crs}, SPHERICAL)")
            self.assertEqual(repr(geography_type), f"GeographyType({srid})")

    def test_geographytype_specified_invalid_srid(self):
        """Test that the correct error is returned when an invalid SRID value is specified."""

        for srid in [-4612, -4326, -2, -1, 1, 2]:
            with self.assertRaises(IllegalArgumentException) as error_context:
                GeographyType(srid)
            srid_header = "[ST_INVALID_SRID_VALUE] Invalid or unsupported SRID"
            self.assertEqual(
                str(error_context.exception),
                f"{srid_header} (spatial reference identifier) value: {srid}."
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
        """Test that two GeographyTypes with specified SRIDs have the same SRID values."""

        for srid in [4326]:
            geography_type_1 = GeographyType(srid)
            geography_type_2 = GeographyType(srid)
            self.assertEqual(geography_type_1.srid, geography_type_2.srid)

    def test_geographytype_different_srid_values(self):
        """Test that two GeographyTypes with specified SRIDs have different SRID values."""

        for srid in [4326]:
            geography_type_1 = GeographyType(srid)
            geography_type_2 = GeographyType("ANY")
            self.assertNotEqual(geography_type_1.srid, geography_type_2.srid)


class GeographyTypeTest(GeographyTypeTestMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_types import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)

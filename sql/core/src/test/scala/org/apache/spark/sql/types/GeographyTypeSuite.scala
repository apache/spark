/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types

import java.util.Locale

import org.json4s.JsonAST.JString

import org.apache.spark.SparkFunSuite
import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalGeographyType}

class GeographyTypeSuite extends SparkFunSuite {

  // These tests verify the basic behavior of the GeographyType logical type.

  test("GEOGRAPHY type with specified invalid SRID") {
    val srids: Seq[Int] = Seq(-4612, -4326, -2, -1, 1, 2, 3126, 4612)
    srids.foreach { srid =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          GeographyType(srid)
        },
        condition = "ST_INVALID_SRID_VALUE",
        sqlState = "22023",
        parameters = Map("srid" -> srid.toString)
      )
    }
  }

  test("GEOGRAPHY type with specified valid SRID") {
    val srids: Seq[Int] = Seq(4326)
    srids.foreach { srid =>
      val g = GeographyType(srid)
      assert(g.srid == srid)
      assert(g == GeographyType(srid))
      assert(g.hashCode() == srid.hashCode())
      // This GEOGRAPHY type has a fixed SRID.
      assert(!g.isMixedSrid)
      // The type name for concrete geography type does display the SRID.
      assert(g.typeName == s"geography($srid)")
      assert(g.simpleString == s"geography($srid)")
      assert(g.sql == s"GEOGRAPHY($srid)")
      // GeographyType with mixed SRID cannot accept any other SRID value.
      assert(g.acceptsGeographyType(GeographyType(4326)))
      assert(!g.acceptsGeographyType(GeographyType("ANY")))
    }
  }

  test("GEOGRAPHY type with specified valid CRS and algorithm") {
    val typeInformation: Seq[(Int, String, EdgeInterpolationAlgorithm)] = Seq(
      (4326, "OGC:CRS84", EdgeInterpolationAlgorithm.SPHERICAL)
    )
    typeInformation.foreach { case (srid, crs, algorithm) =>
      val g = GeographyType(crs, algorithm)
      // Verify that the type is correctly created.
      assert(g.srid == srid)
      assert(g.crs == crs)
      assert(g.algorithm == algorithm)
      assert(g == GeographyType(srid))
      assert(g.hashCode() == srid.hashCode())
      // This GEOGRAPHY type has a fixed SRID.
      assert(!g.isMixedSrid)
      // The type name for concrete geography type does display the SRID.
      assert(g.typeName == s"geography($srid)")
      assert(g.simpleString == s"geography($srid)")
      assert(g.sql == s"GEOGRAPHY($srid)")
      // GeographyType with mixed SRID cannot accept any other SRID value.
      assert(g.acceptsGeographyType(GeographyType(4326)))
      assert(!g.acceptsGeographyType(GeographyType("ANY")))
    }
  }

  test("GEOGRAPHY type with the special ANY specifier for mixed SRID") {
    val g = GeographyType("ANY")
    assert(g.srid == GeographyType.MIXED_SRID)
    assert(g == GeographyType("ANY"))
    assert(g.hashCode() == GeographyType.MIXED_SRID.hashCode())
    // This GEOGRAPHY type has a fixed SRID.
    assert(g.isMixedSrid)
    // The type name for concrete geography type does display the SRID.
    assert(g.typeName == s"geography(any)")
    assert(g.simpleString == s"geography(any)")
    assert(g.sql == s"GEOGRAPHY(ANY)")
    // GeographyType with mixed SRID can accept any other SRID value.
    assert(g.acceptsGeographyType(GeographyType(4326)))
    assert(g.acceptsGeographyType(GeographyType("ANY")))
  }

  // These tests verify the interaction between different GeographyTypes.

  test("GEOGRAPHY types with same SRID values") {
    val g1 = GeographyType(4326)
    val g2 = GeographyType(4326)
    // These two GEOGRAPHY types have equal type info.
    assert(g1.srid == g2.srid)
    assert(g1.crs == g2.crs)
    assert(g1.algorithm == g2.algorithm)
    // These two GEOGRAPHY types are considered equal.
    assert(g1 == g2)
    // These two GEOGRAPHY types can accept each other.
    assert(g1.acceptsGeographyType(g2))
    assert(g2.acceptsGeographyType(g1))
  }

  // This test verifies the SQL and JSON representation of GEOGRAPHY types.

  test("GEOGRAPHY data type representation") {
    def assertStringRepresentation(
        geomType: GeographyType,
        typeName: String,
        jsonValue: String): Unit = {
      assert(geomType.typeName === typeName)
      assert(geomType.sql === typeName.toUpperCase(Locale.ROOT))
      assert(geomType.jsonValue === JString(jsonValue))
    }
    assertStringRepresentation(
      GeographyType(4326),
      "geography(4326)",
      "geography(OGC:CRS84, SPHERICAL)"
    )
  }

  // These tests verify the JSON parsing of different GEOGRAPHY types.

  test("GEOGRAPHY data type JSON parsing with valid CRS and algorithm") {
    val validGeographies = Seq(
      "\"geography\"",
      "\"geography(OGC:CRS84)\"",
      "\"geography(   OGC:CRS84 )\"",
      "\"geography(spherical)\"",
      "\"geography(SPHERICAL)\"",
      "\"geography(  spherical)\"",
      "\"geography(OGC:CRS84,    spherical    )\"",
      "\"geography( OGC:CRS84   , spherical )\"",
      "\"geography( OGC:CRS84   , SPHERICAL )\""
    )
    validGeographies.foreach { geog =>
      DataType.fromJson(geog).isInstanceOf[GeographyType]
    }
  }

  test("GEOGRAPHY data type JSON parsing with invalid CRS or algorithm") {
    val invalidGeographies = Seq(
      "\"geography()\"",
      "\"geography(())\"",
      "\"geography(asdf)\"",
      "\"geography(srid:0)\"",
      "\"geography(123:123)\"",
      "\"geography(srid:srid)\"",
      "\"geography(karney)\"",
      "\"geography(srid:srid, spherical)\"",
      "\"geography(OGC:CRS84, karney)\""
    )
    invalidGeographies.foreach { geog =>
      val exception = intercept[SparkIllegalArgumentException] {
        DataType.fromJson(geog)
      }
      assert(
        Seq(
          "INVALID_JSON_DATA_TYPE",
          "ST_INVALID_CRS_VALUE",
          "ST_INVALID_ALGORITHM_VALUE"
        ).contains(exception.getCondition)
      )
    }
  }

  // These tests verify the SQL parsing of different GEOGRAPHY types.

  test("GEOGRAPHY data type SQL parsing with valid SRID") {
    val validGeographies = Seq(
      "GEOGRAPHY(ANY)",
      "GEOGRAPHY(4326)"
    )
    validGeographies.foreach { geog =>
      val dt = DataType.fromDDL(geog)
      assert(dt.isInstanceOf[GeographyType])
    }
  }

  test("GEOGRAPHY data type SQL parsing with invalid SRID") {
    val invalidGeographies = Seq(
      "GEOGRAPHY(123)",
      "GEOGRAPHY(-1)",
      "GEOGRAPHY(-4326)",
      "GEOGRAPHY(99999)",
      "GEOGRAPHY(SRID)",
      "GEOGRAPHY(MIXED)"
    )
    invalidGeographies.foreach { geog =>
      val exception = intercept[Exception] {
        DataType.fromDDL(geog)
      }
      exception match {
        case e: SparkIllegalArgumentException =>
          assert(e.getCondition == "ST_INVALID_SRID_VALUE")
        case e: org.apache.spark.sql.catalyst.parser.ParseException =>
          assert(e.getMessage.contains("PARSE_SYNTAX_ERROR"))
        case _ =>
          fail(s"Unexpected exception type: ${exception.getClass.getName}")
      }
    }
  }

  test("PhysicalDataType maps GeographyType to PhysicalGeographyType") {
    val geometryTypes: Seq[DataType] = Seq(
      GeographyType(4326),
      GeographyType("ANY")
    )
    geometryTypes.foreach { geometryType =>
      val pdt = PhysicalDataType(geometryType)
      assert(pdt.isInstanceOf[PhysicalGeographyType])
    }
  }
}

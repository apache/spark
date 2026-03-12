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
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalGeometryType}

class GeometryTypeSuite extends SparkFunSuite {

  // These tests verify the basic behavior of the GeometryType logical type.

  test("GEOMETRY type with specified invalid SRID") {
    val srids: Seq[Int] = Seq(-4612, -4326, -2, -1, 1, 2)
    srids.foreach { srid =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          GeometryType(srid)
        },
        condition = "ST_INVALID_SRID_VALUE",
        sqlState = "22023",
        parameters = Map("srid" -> srid.toString)
      )
    }
  }

  test("GEOMETRY type with specified valid SRID") {
    val srids: Seq[Int] = Seq(0, 3857, 4326)
    srids.foreach { srid =>
      val g = GeometryType(srid)
      assert(g.srid == srid)
      assert(g == GeometryType(srid))
      assert(g.hashCode() == srid.hashCode())
      // This GEOMETRY type has a fixed SRID.
      assert(!g.isMixedSrid)
      // The type name for concrete geometry type does display the SRID.
      assert(g.typeName == s"geometry($srid)")
      assert(g.simpleString == s"geometry($srid)")
      assert(g.sql == s"GEOMETRY($srid)")
      // GeometryType with a specific SRID cannot accept a different SRID value.
      val otherSrid = if (srid == 3857) 4326 else 3857
      assert(!g.acceptsGeometryType(GeometryType(otherSrid)))
    }
  }

  test("GEOMETRY type with the special ANY specifier for mixed SRID") {
    val g = GeometryType("ANY")
    assert(g.srid == GeometryType.MIXED_SRID)
    assert(g == GeometryType("ANY"))
    assert(g.hashCode() == GeometryType.MIXED_SRID.hashCode())
    // This GEOMETRY type has a fixed SRID.
    assert(g.isMixedSrid)
    // The type name for concrete geometry type does display the SRID.
    assert(g.typeName == s"geometry(any)")
    assert(g.simpleString == s"geometry(any)")
    assert(g.sql == s"GEOMETRY(ANY)")
    // GeometryType with mixed SRID can accept any other SRID value.
    assert(g.acceptsGeometryType(GeometryType(0)))
    assert(g.acceptsGeometryType(GeometryType(3857)))
    assert(g.acceptsGeometryType(GeometryType(4326)))
  }

  // These tests verify the interaction between different GeometryTypes.

  test("GEOMETRY types with same SRID values") {
    val g1 = GeometryType(4326)
    val g2 = GeometryType(4326)
    // These two GEOMETRY types have equal type info.
    assert(g1.srid == g2.srid)
    assert(g1.crs == g2.crs)
    // These two GEOMETRY types are considered equal.
    assert(g1 == g2)
    // These two GEOMETRY types can accept each other.
    assert(g1.acceptsGeometryType(g2))
    assert(g2.acceptsGeometryType(g1))
  }

  test("GEOMETRY types with different SRID values") {
    val g1 = GeometryType(4326)
    val g2 = GeometryType(3857)
    // These two GEOMETRY types have different type info.
    assert(g1.srid != g2.srid)
    assert(g1.crs != g2.crs)
    // These two GEOMETRY types are considered different.
    assert(g1 != g2)
    // These two GEOMETRY types cannot accept each other.
    assert(!g1.acceptsGeometryType(g2))
    assert(!g2.acceptsGeometryType(g1))
  }

  // This test verifies the SQL and JSON representation of GEOMETRY types.

  test("GEOMETRY data type representation") {
    def assertStringRepresentation(
        geomType: GeometryType,
        typeName: String,
        jsonValue: String): Unit = {
      assert(geomType.typeName === typeName)
      assert(geomType.sql === typeName.toUpperCase(Locale.ROOT))
      assert(geomType.jsonValue === JString(jsonValue))
    }
    assertStringRepresentation(GeometryType(0), "geometry(0)", "geometry(SRID:0)")
    assertStringRepresentation(GeometryType(3857), "geometry(3857)", "geometry(EPSG:3857)")
    assertStringRepresentation(GeometryType(4326), "geometry(4326)", "geometry(OGC:CRS84)")
  }

  // These tests verify the JSON parsing of different GEOMETRY types.

  test("GEOMETRY data type JSON parsing with valid CRS") {
    val validGeometries = Seq(
      "\"geometry\"",
      "\"geometry(OGC:CRS84)\""
    )
    validGeometries.foreach { geom =>
      DataType.fromJson(geom).isInstanceOf[GeometryType]
    }
  }

  test("GEOMETRY data type JSON parsing with invalid CRS") {
    val invalidGeometries = Seq(
      "\"geometry()\"",
      "\"geometry(())\"",
      "\"geometry(asdf)\"",
      "\"geometry(asdf:fdsa)\"",
      "\"geometry(123:123)\"",
      "\"geometry(srid:srid)\"",
      "\"geometry(SRID:1)\"",
      "\"geometry(SRID:123)\"",
      "\"geometry(EPSG:123)\"",
      "\"geometry(ESRI:123)\"",
      "\"geometry(OCG:123)\"",
      "\"geometry(OCG:CRS123)\""
    )
    invalidGeometries.foreach { geom =>
      val exception = intercept[SparkIllegalArgumentException] {
        DataType.fromJson(geom)
      }
      assert(
        Seq(
          "INVALID_JSON_DATA_TYPE",
          "ST_INVALID_CRS_VALUE"
        ).contains(exception.getCondition)
      )
    }
  }

  // These tests verify the SQL parsing of different GEOMETRY types.

  test("GEOMETRY data type SQL parsing with valid SRID") {
    val validGeometries = Seq(
      "GEOMETRY(ANY)",
      "GEOMETRY(0)",
      "GEOMETRY(3857)",
      "GEOMETRY(4326)"
    )
    validGeometries.foreach { geom =>
      val dt = DataType.fromDDL(geom)
      assert(dt.isInstanceOf[GeometryType])
    }
  }

  test("GEOMETRY data type SQL parsing with invalid SRID") {
    val invalidGeometries = Seq(
      "GEOMETRY(123)",
      "GEOMETRY(-1)",
      "GEOMETRY(-4326)",
      "GEOMETRY(99999)",
      "GEOMETRY(SRID)",
      "GEOMETRY(MIXED)"
    )
    invalidGeometries.foreach { geom =>
      val exception = intercept[Exception] {
        DataType.fromDDL(geom)
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

  test("PhysicalDataType maps GeometryType to PhysicalGeometryType") {
    val geometryTypes: Seq[DataType] = Seq(
      GeometryType(0),
      GeometryType(3857),
      GeometryType(4326),
      GeometryType("ANY")
    )
    geometryTypes.foreach { geometryType =>
      val pdt = PhysicalDataType(geometryType)
      assert(pdt.isInstanceOf[PhysicalGeometryType])
    }
  }
}

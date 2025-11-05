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

package org.apache.spark.sql

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.st._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class STExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  // Private common constants used across several tests.
  private final val defaultGeographySrid: Int = ExpressionDefaults.DEFAULT_GEOGRAPHY_SRID
  private final val defaultGeographyType: DataType = GeographyType(defaultGeographySrid)
  private final val defaultGeometrySrid: Int = ExpressionDefaults.DEFAULT_GEOMETRY_SRID
  private final val defaultGeometryType: DataType = GeometryType(defaultGeometrySrid)

  // Private helper method to assert the data type of a query result.
  private def assertType(query: String, expectedDataType: DataType) = {
    assert(sql(query).schema.fields.head.dataType.sameType(expectedDataType))
  }

  /** ST reader/writer expressions. */

  test("ST_AsBinary") {
    // Test data: WKB representation of POINT(1 2).
    val wkb = Hex.unhex("0101000000000000000000F03F0000000000000040".getBytes())
    val wkbLiteral = Literal.create(wkb, BinaryType)
    // ST_GeogFromWKB and ST_AsBinary.
    val geographyExpression = ST_GeogFromWKB(wkbLiteral)
    assert(geographyExpression.dataType.sameType(defaultGeographyType))
    checkEvaluation(ST_AsBinary(geographyExpression), wkb)
    // ST_GeomFromWKB and ST_AsBinary.
    val geometryExpression = ST_GeomFromWKB(wkbLiteral)
    assert(geometryExpression.dataType.sameType(defaultGeometryType))
    checkEvaluation(ST_AsBinary(geometryExpression), wkb)
  }

  /** ST accessor expressions. */

  test("ST_Srid") {
    // Test data: WKB representation of POINT(1 2).
    val wkb = Hex.unhex("0101000000000000000000F03F0000000000000040".getBytes())
    val wkbLiteral = Literal.create(wkb, BinaryType)

    // ST_Srid with GEOGRAPHY.
    val geographyExpression = ST_GeogFromWKB(wkbLiteral)
    val stSridGeography = ST_Srid(geographyExpression)
    assert(stSridGeography.dataType.sameType(IntegerType))
    checkEvaluation(stSridGeography, defaultGeographySrid)
    // Test NULL handling.
    val nullGeographyLiteral = Literal.create(null, defaultGeographyType)
    val stSridGeographyNull = ST_Srid(nullGeographyLiteral)
    assert(stSridGeographyNull.dataType.sameType(IntegerType))
    checkEvaluation(stSridGeographyNull, null)

    // ST_Srid with GEOMETRY.
    val geometryExpression = ST_GeomFromWKB(wkbLiteral)
    val stSridGeometry = ST_Srid(geometryExpression)
    assert(stSridGeometry.dataType.sameType(IntegerType))
    checkEvaluation(stSridGeometry, defaultGeometrySrid)
    // Test NULL handling.
    val nullGeometryLiteral = Literal.create(null, defaultGeometryType)
    val stSridGeometryNull = ST_Srid(nullGeometryLiteral)
    assert(stSridGeometryNull.dataType.sameType(IntegerType))
    checkEvaluation(stSridGeometryNull, null)
  }

  /** ST modifier expressions. */

  test("ST_SetSrid - expressions") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    val wkb = Hex.unhex(wkbString.getBytes())
    val wkbLiteral = Literal.create(wkb, BinaryType)
    val geographyLiteral = ST_GeogFromWKB(wkbLiteral)
    val nullGeographyLiteral = Literal.create(null, defaultGeographyType)
    val geometryLiteral = ST_GeomFromWKB(wkbLiteral)
    val nullGeometryLiteral = Literal.create(null, defaultGeometryType)
    val srid = 4326
    val sridLiteral = Literal.create(srid, IntegerType)
    val nullSridLiteral = Literal.create(null, IntegerType)
    val invalidSrid = 9999
    val invalidSridLiteral = Literal.create(9999, IntegerType)

    // ST_SetSrid on GEOGRAPHY expression.
    val geogLit = ST_SetSrid(geographyLiteral, sridLiteral)
    assert(geogLit.dataType.sameType(GeographyType(srid)))
    checkEvaluation(ST_AsBinary(geogLit), wkb)
    val geogLitSrid = ST_Srid(geogLit)
    assert(geogLitSrid.dataType.sameType(IntegerType))
    checkEvaluation(geogLitSrid, srid)
    // Test NULL handling on GEOGRAPHY.
    val nullGeog = ST_SetSrid(nullGeographyLiteral, sridLiteral)
    assert(nullGeog.dataType.sameType(GeographyType(srid)))
    checkEvaluation(nullGeog, null)
    val geogNullSrid = ST_SetSrid(geographyLiteral, nullSridLiteral)
    assert(geogNullSrid.dataType.sameType(GeographyType("ANY")))
    checkEvaluation(geogNullSrid, null)
    // Test error handling for invalid SRID.
    val geogInvalidSrid = ST_SetSrid(geographyLiteral, invalidSridLiteral)
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        geogInvalidSrid.eval()
      },
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> s"$invalidSrid")
    )

    // ST_SetSrid on GEOMETRY expression.
    val geomLit = ST_SetSrid(geometryLiteral, sridLiteral)
    assert(geomLit.dataType.sameType(GeometryType(srid)))
    checkEvaluation(ST_AsBinary(geomLit), wkb)
    val geomLitSrid = ST_Srid(geomLit)
    assert(geomLitSrid.dataType.sameType(IntegerType))
    checkEvaluation(geomLitSrid, srid)
    // Test NULL handling on GEOMETRY.
    val nullGeom = ST_SetSrid(nullGeometryLiteral, sridLiteral)
    assert(nullGeom.dataType.sameType(GeometryType(srid)))
    checkEvaluation(nullGeom, null)
    val geomNullSrid = ST_SetSrid(geometryLiteral, nullSridLiteral)
    assert(geomNullSrid.dataType.sameType(GeometryType("ANY")))
    checkEvaluation(geomNullSrid, null)
    // Test error handling for invalid SRID.
    val geomInvalidSrid = ST_SetSrid(geometryLiteral, invalidSridLiteral)
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        geomInvalidSrid.eval()
      },
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> s"$invalidSrid")
    )
  }

  test("ST_SetSrid - columns") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    val srid = 4326

    withTable("tbl") {
      // Construct the test table.
      sql(s"CREATE TABLE tbl (wkb BINARY, srid INT)")
      sql(s"INSERT INTO tbl VALUES (X'$wkbString', $srid)")

      // ST_SetSrid on GEOGRAPHY column value, with SRID column value.
      val geogColSridCol = "ST_SetSrid(ST_GeogFromWKB(wkb), srid)"
      assertType(s"SELECT $geogColSridCol FROM tbl", GeographyType("ANY"))
      assertType(s"SELECT ST_Srid($geogColSridCol) FROM tbl", IntegerType)
      checkAnswer(sql(s"SELECT ST_Srid($geogColSridCol) FROM tbl"), Row(srid))
      // ST_SetSrid on GEOMETRY column value, with SRID column value.
      val geomColSridCol = "ST_SetSrid(ST_GeomFromWKB(wkb), srid)"
      assertType(s"SELECT $geomColSridCol FROM tbl", GeometryType("ANY"))
      assertType(s"SELECT ST_Srid($geomColSridCol) FROM tbl", IntegerType)
      checkAnswer(sql(s"SELECT ST_Srid($geomColSridCol) FROM tbl"), Row(srid))

      // ST_SetSrid on GEOGRAPHY literal value, with SRID column value.
      val geogLitSridCol = s"ST_SetSrid(ST_GeogFromWKB(X'$wkbString'), srid)"
      assertType(s"SELECT $geogLitSridCol FROM tbl", GeographyType("ANY"))
      assertType(s"SELECT ST_Srid($geogLitSridCol) FROM tbl", IntegerType)
      checkAnswer(sql(s"SELECT ST_Srid($geogLitSridCol) FROM tbl"), Row(srid))
      // ST_SetSrid on GEOMETRY literal value, with SRID column value.
      val geomLitSridCol = s"ST_SetSrid(ST_GeomFromWKB(X'$wkbString'), srid)"
      assertType(s"SELECT $geomLitSridCol FROM tbl", GeometryType("ANY"))
      assertType(s"SELECT ST_Srid($geomLitSridCol) FROM tbl", IntegerType)
      checkAnswer(sql(s"SELECT ST_Srid($geomLitSridCol) FROM tbl"), Row(srid))

      // ST_SetSrid on GEOGRAPHY column value, with SRID literal.
      val geogColSridLit = s"ST_SetSrid(ST_GeogFromWKB(wkb), $srid)"
      assertType(s"SELECT $geogColSridLit FROM tbl", GeographyType(srid))
      assertType(s"SELECT ST_Srid($geogColSridLit) FROM tbl", IntegerType)
      checkAnswer(sql(s"SELECT ST_Srid($geogColSridLit) FROM tbl"), Row(srid))
      // ST_SetSrid on GEOMETRY column value, with SRID literal.
      val geomColSridLit = s"ST_SetSrid(ST_GeomFromWKB(wkb), $srid)"
      assertType(s"SELECT $geomColSridLit FROM tbl", GeometryType(srid))
      assertType(s"SELECT ST_Srid($geomColSridLit) FROM tbl", IntegerType)
      checkAnswer(sql(s"SELECT ST_Srid($geomColSridLit) FROM tbl"), Row(srid))

      // ST_SetSrid on GEOGRAPHY literal value, with SRID literal.
      val geogLitSridLit = s"ST_SetSrid(ST_GeogFromWKB(X'$wkbString'), $srid)"
      assertType(s"SELECT $geogLitSridLit FROM tbl", GeographyType(srid))
      assertType(s"SELECT ST_Srid($geogLitSridLit) FROM tbl", IntegerType)
      checkAnswer(sql(s"SELECT ST_Srid($geogLitSridLit) FROM tbl"), Row(srid))
      // ST_SetSrid on GEOMETRY literal value, with SRID literal.
      val geomLitSridLit = s"ST_SetSrid(ST_GeomFromWKB(X'$wkbString'), $srid)"
      assertType(s"SELECT $geomLitSridLit FROM tbl", GeometryType(srid))
      assertType(s"SELECT ST_Srid($geomLitSridLit) FROM tbl", IntegerType)
      checkAnswer(sql(s"SELECT ST_Srid($geomLitSridLit) FROM tbl"), Row(srid))
    }
  }

}

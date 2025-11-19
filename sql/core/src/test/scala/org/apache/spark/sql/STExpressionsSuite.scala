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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class STExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  // Private common constants used across several tests.
  private final val defaultGeographySrid: Int = ExpressionDefaults.DEFAULT_GEOGRAPHY_SRID
  private final val defaultGeographyType: DataType = GeographyType(defaultGeographySrid)
  private final val mixedSridGeographyType: DataType = GeographyType("ANY")
  private final val defaultGeometrySrid: Int = ExpressionDefaults.DEFAULT_GEOMETRY_SRID
  private final val defaultGeometryType: DataType = GeometryType(defaultGeometrySrid)
  private final val mixedSridGeometryType: DataType = GeometryType("ANY")

  // Private helper method to assert the data type of a query result.
  private def assertType(query: String, expectedDataType: DataType) = {
    assert(sql(query).schema.fields.head.dataType.sameType(expectedDataType))
  }

  /** Geospatial type casting. */

  test("Cast GEOGRAPHY(srid) to GEOGRAPHY(ANY)") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    val wkb = Hex.unhex(wkbString.getBytes())
    val wkbLiteral = Literal.create(wkb, BinaryType)

    // Construct the input GEOGRAPHY expression.
    val geogExpr = ST_GeogFromWKB(wkbLiteral)
    assert(geogExpr.dataType.sameType(defaultGeographyType))
    checkEvaluation(ST_AsBinary(geogExpr), wkb)
    // Cast the GEOGRAPHY with fixed SRID to GEOGRAPHY with mixed SRID.
    val castExpr = Cast(geogExpr, mixedSridGeographyType)
    assert(castExpr.dataType.sameType(mixedSridGeographyType))
    checkEvaluation(ST_AsBinary(castExpr), wkb)

    // Construct the input GEOGRAPHY SQL query, using WKB literal.
    val geogQueryLit: String = s"ST_GeogFromWKB(X'$wkbString')"
    assertType(s"SELECT $geogQueryLit", defaultGeographyType)
    checkAnswer(sql(s"SELECT ST_AsBinary($geogQueryLit)"), Row(wkb))
    // Cast the GEOGRAPHY with fixed SRID to GEOGRAPHY with mixed SRID.
    val castQueryLit = s"$geogQueryLit::GEOGRAPHY(ANY)"
    assertType(s"SELECT $castQueryLit", mixedSridGeographyType)
    checkAnswer(sql(s"SELECT ST_AsBinary($castQueryLit)"), Row(wkb))

    withTable("tbl") {
      // Construct the test table with WKB.
      sql(s"CREATE TABLE tbl (wkb BINARY)")
      sql(s"INSERT INTO tbl VALUES (X'$wkbString')")

      // Construct the input GEOGRAPHY SQL query, using WKB column.
      val geogQueryCol: String = s"ST_GeogFromWKB(wkb)"
      assertType(s"SELECT $geogQueryCol FROM tbl", defaultGeographyType)
      checkAnswer(sql(s"SELECT ST_AsBinary($geogQueryCol) FROM tbl"), Row(wkb))
      // Cast the GEOGRAPHY with fixed SRID to GEOGRAPHY with mixed SRID.
      val castQueryCol = s"$geogQueryCol::GEOGRAPHY(ANY)"
      assertType(s"SELECT $castQueryCol FROM tbl", mixedSridGeographyType)
      checkAnswer(sql(s"SELECT ST_AsBinary($castQueryCol) FROM tbl"), Row(wkb))
    }
  }

  test("Cast GEOMETRY(srid) to GEOMETRY(ANY)") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    val wkb = Hex.unhex(wkbString.getBytes())
    val wkbLiteral = Literal.create(wkb, BinaryType)

    // Construct the input GEOMETRY expression.
    val geomExpr = ST_GeomFromWKB(wkbLiteral)
    assert(geomExpr.dataType.sameType(defaultGeometryType))
    checkEvaluation(ST_AsBinary(geomExpr), wkb)
    // Cast the GEOMETRY with fixed SRID to GEOMETRY with mixed SRID.
    val castExpr = Cast(geomExpr, mixedSridGeometryType)
    assert(castExpr.dataType.sameType(mixedSridGeometryType))
    checkEvaluation(ST_AsBinary(castExpr), wkb)

    // Construct the input GEOMETRY SQL query, using WKB literal.
    val geomQueryLit: String = s"ST_GeomFromWKB(X'$wkbString')"
    assertType(s"SELECT $geomQueryLit", defaultGeometryType)
    checkAnswer(sql(s"SELECT ST_AsBinary($geomQueryLit)"), Row(wkb))
    // Cast the GEOMETRY with fixed SRID to GEOMETRY with mixed SRID.
    val castQueryLit = s"$geomQueryLit::GEOMETRY(ANY)"
    assertType(s"SELECT $castQueryLit", mixedSridGeometryType)
    checkAnswer(sql(s"SELECT ST_AsBinary($castQueryLit)"), Row(wkb))

    withTable("tbl") {
      // Construct the test table with WKB.
      sql(s"CREATE TABLE tbl (wkb BINARY)")
      sql(s"INSERT INTO tbl VALUES (X'$wkbString')")

      // Construct the input GEOMETRY SQL query, using WKB column.
      val geomQueryCol: String = s"ST_GeomFromWKB(wkb)"
      assertType(s"SELECT $geomQueryCol FROM tbl", defaultGeometryType)
      checkAnswer(sql(s"SELECT ST_AsBinary($geomQueryCol) FROM tbl"), Row(wkb))
      // Cast the GEOMETRY with fixed SRID to GEOMETRY with mixed SRID.
      val castQueryCol = s"$geomQueryCol::GEOMETRY(ANY)"
      assertType(s"SELECT $castQueryCol FROM tbl", mixedSridGeometryType)
      checkAnswer(sql(s"SELECT ST_AsBinary($castQueryCol) FROM tbl"), Row(wkb))
    }
  }

  /** Geospatial type coercion. */

  test("CreateArray with GEOGRAPHY literals") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    // Test with literals, using geographies with different SRID values.
    val geog1 = s"ST_GeogFromWKB(X'$wkbString')" // Literal with fixed SRID (4326).
    val geographyType1 = GeographyType(4326)
    val geog2 = s"$geog1::GEOGRAPHY(ANY)" // Literal with mixed SRID (ANY).
    val geographyType2 = GeographyType("ANY")
    val geo = "hex(ST_AsBinary(g)), ST_Srid(g)"
    val row = Row(wkbString, 4326)

    val testCases = Seq(
      (s"array($geog1)", geographyType1, Seq(row)),
      (s"array($geog2)", geographyType2, Seq(row)),
      (s"array($geog1, $geog1)", geographyType1, Seq(row, row)),
      (s"array($geog2, $geog2)", geographyType2, Seq(row, row)),
      (s"array($geog1, $geog2)", mixedSridGeographyType, Seq(row, row)),
      (s"array($geog2, $geog1)", mixedSridGeographyType, Seq(row, row))
    )

    for ((expr, expectedType, expectedRows) <- testCases) {
      assertType(
        s"SELECT $expr",
        ArrayType(expectedType)
      )
      checkAnswer(
        sql(s"WITH t AS (SELECT explode($expr) AS g) SELECT $geo FROM t"),
        expectedRows
      )
    }
  }

  test("CreateArray with GEOGRAPHY columns") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    // Test with columns, using geographies with different SRID values.
    val geog1 = "ST_GeogFromWKB(wkb)" // Column with fixed SRID (4326).
    val geographyType1 = GeographyType(4326)
    val geog2 = s"$geog1::GEOGRAPHY(ANY)" // Column with mixed SRID (ANY).
    val geographyType2 = GeographyType("ANY")
    val geo = "hex(ST_AsBinary(g)), ST_Srid(g)"
    val row = Row(wkbString, 4326)

    val testCases = Seq(
      (s"array($geog1)", geographyType1, Seq(row)),
      (s"array($geog2)", geographyType2, Seq(row)),
      (s"array($geog1, $geog1)", geographyType1, Seq(row, row)),
      (s"array($geog2, $geog2)", geographyType2, Seq(row, row)),
      (s"array($geog1, $geog2)", mixedSridGeographyType, Seq(row, row)),
      (s"array($geog2, $geog1)", mixedSridGeographyType, Seq(row, row))
    )

    // Test with literal and column, using geographies with different SRID values.
    withTable("tbl") {
      // Construct and populate the test table.
      sql("CREATE TABLE tbl (wkb BINARY)")
      sql(s"INSERT INTO tbl VALUES (X'$wkbString')")

      for ((query, expectedType, expectedRows) <- testCases) {
        assertType(
          s"SELECT $query FROM tbl",
          ArrayType(expectedType)
        )
        checkAnswer(
          sql(s"WITH t AS (SELECT explode($query) AS g FROM tbl) SELECT $geo FROM t"),
          expectedRows
        )
      }
    }
  }

  test("NVL with GEOGRAPHY literals") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    // Test with literals, using geographies with different SRID values.
    val geog1 = s"ST_GeogFromWKB(X'$wkbString')" // Literal with fixed SRID (4326).
    val geographyType1 = GeographyType(4326)
    val geog2 = s"$geog1::GEOGRAPHY(ANY)" // Literal with mixed SRID (ANY).
    val geographyType2 = GeographyType("ANY")
    val geo = "hex(ST_AsBinary(g)), ST_Srid(g)"
    val row = Row(wkbString, 4326)

    val testCases = Seq(
      (s"nvl(null, $geog1)", geographyType1, Seq(row)),
      (s"nvl($geog1, null)", geographyType1, Seq(row)),
      (s"nvl(null, $geog2)", geographyType2, Seq(row)),
      (s"nvl($geog2, null)", geographyType2, Seq(row)),
      (s"nvl($geog1, $geog1)", geographyType1, Seq(row)),
      (s"nvl($geog2, $geog2)", geographyType2, Seq(row)),
      (s"nvl($geog1, $geog2)", mixedSridGeographyType, Seq(row)),
      (s"nvl($geog2, $geog1)", mixedSridGeographyType, Seq(row))
    )

    for ((expr, expectedType, expectedRows) <- testCases) {
      assertType(
        s"SELECT $expr",
        expectedType
      )
      checkAnswer(
        sql(s"WITH t AS (SELECT $expr AS g) SELECT $geo FROM t"),
        expectedRows
      )
    }
  }

  test("NVL with GEOGRAPHY columns") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    // Test with columns, using geographies with different SRID values.
    val geog1 = "ST_GeogFromWKB(wkb)" // Column with fixed SRID (4326).
    val geographyType1 = GeographyType(4326)
    val geog2 = s"$geog1::GEOGRAPHY(ANY)" // Column with mixed SRID (ANY).
    val geographyType2 = GeographyType("ANY")
    val geo = "hex(ST_AsBinary(g)), ST_Srid(g)"
    val row = Row(wkbString, 4326)

    val testCases = Seq(
      (s"nvl(null, $geog1)", geographyType1, Seq(row)),
      (s"nvl($geog1, null)", geographyType1, Seq(row)),
      (s"nvl(null, $geog2)", geographyType2, Seq(row)),
      (s"nvl($geog2, null)", geographyType2, Seq(row)),
      (s"nvl($geog1, $geog1)", geographyType1, Seq(row)),
      (s"nvl($geog2, $geog2)", geographyType2, Seq(row)),
      (s"nvl($geog1, $geog2)", mixedSridGeographyType, Seq(row)),
      (s"nvl($geog2, $geog1)", mixedSridGeographyType, Seq(row))
    )

    // Test with literal and column, using geographies with different SRID values.
    withTable("tbl") {
      // Construct and populate the test table.
      sql("CREATE TABLE tbl (wkb BINARY)")
      sql(s"INSERT INTO tbl VALUES (X'$wkbString')")

      for ((query, expectedType, expectedRows) <- testCases) {
        assertType(
          s"SELECT $query FROM tbl",
          expectedType
        )
        checkAnswer(
          sql(s"WITH t AS (SELECT $query AS g FROM tbl) SELECT $geo FROM t"),
          expectedRows
        )
      }
    }
  }

  test("CreateArray with GEOMETRY literals") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    // Test with literals, using geometries with different SRID values.
    val geom1 = s"ST_GeomFromWKB(X'$wkbString')" // Literal with fixed SRID (0).
    val geometryType1 = GeometryType(0)
    val geom2 = s"$geom1::GEOMETRY(ANY)" // Literal with mixed SRID (ANY).
    val geometryType2 = GeometryType("ANY")
    val geo = "hex(ST_AsBinary(g)), ST_Srid(g)"
    val row = Row(wkbString, 0)

    val testCases = Seq(
      (s"array($geom1)", geometryType1, Seq(row)),
      (s"array($geom2)", geometryType2, Seq(row)),
      (s"array($geom1, $geom1)", geometryType1, Seq(row, row)),
      (s"array($geom2, $geom2)", geometryType2, Seq(row, row)),
      (s"array($geom1, $geom2)", mixedSridGeometryType, Seq(row, row)),
      (s"array($geom2, $geom1)", mixedSridGeometryType, Seq(row, row))
    )

    for ((expr, expectedType, expectedRows) <- testCases) {
      assertType(
        s"SELECT $expr",
        ArrayType(expectedType)
      )
      checkAnswer(
        sql(s"WITH t AS (SELECT explode($expr) AS g) SELECT $geo FROM t"),
        expectedRows
      )
    }
  }

  test("CreateArray with GEOMETRY columns") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    // Test with columns, using geometries with different SRID values.
    val geom1 = "ST_GeomFromWKB(wkb)" // Column with fixed SRID (0).
    val geometryType1 = GeometryType(0)
    val geom2 = s"$geom1::GEOMETRY(ANY)" // Column with mixed SRID (ANY).
    val geometryType2 = GeometryType("ANY")
    val geo = "hex(ST_AsBinary(g)), ST_Srid(g)"
    val row = Row(wkbString, 0)

    val testCases = Seq(
      (s"array($geom1)", geometryType1, Seq(row)),
      (s"array($geom2)", geometryType2, Seq(row)),
      (s"array($geom1, $geom1)", geometryType1, Seq(row, row)),
      (s"array($geom2, $geom2)", geometryType2, Seq(row, row)),
      (s"array($geom1, $geom2)", mixedSridGeometryType, Seq(row, row)),
      (s"array($geom2, $geom1)", mixedSridGeometryType, Seq(row, row))
    )

    // Test with literal and column, using geometries with different SRID values.
    withTable("tbl") {
      // Construct and populate the test table.
      sql("CREATE TABLE tbl (wkb BINARY)")
      sql(s"INSERT INTO tbl VALUES (X'$wkbString')")

      for ((query, expectedType, expectedRows) <- testCases) {
        assertType(
          s"SELECT $query FROM tbl",
          ArrayType(expectedType)
        )
        checkAnswer(
          sql(s"WITH t AS (SELECT explode($query) AS g FROM tbl) SELECT $geo FROM t"),
          expectedRows
        )
      }
    }
  }

  test("NVL with GEOMETRY literals") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    // Test with literals, using geometries with different SRID values.
    val geom1 = s"ST_GeomFromWKB(X'$wkbString')" // Literal with fixed SRID (0).
    val geometryType1 = GeometryType(0)
    val geom2 = s"$geom1::GEOMETRY(ANY)" // Literal with mixed SRID (ANY).
    val geometryType2 = GeometryType("ANY")
    val geo = "hex(ST_AsBinary(g)), ST_Srid(g)"
    val row = Row(wkbString, 0)

    val testCases = Seq(
      (s"nvl(null, $geom1)", geometryType1, Seq(row)),
      (s"nvl($geom1, null)", geometryType1, Seq(row)),
      (s"nvl(null, $geom2)", geometryType2, Seq(row)),
      (s"nvl($geom2, null)", geometryType2, Seq(row)),
      (s"nvl($geom1, $geom1)", geometryType1, Seq(row)),
      (s"nvl($geom2, $geom2)", geometryType2, Seq(row)),
      (s"nvl($geom1, $geom2)", mixedSridGeometryType, Seq(row)),
      (s"nvl($geom2, $geom1)", mixedSridGeometryType, Seq(row))
    )

    for ((expr, expectedType, expectedRows) <- testCases) {
      assertType(
        s"SELECT $expr",
        expectedType
      )
      checkAnswer(
        sql(s"WITH t AS (SELECT $expr AS g) SELECT $geo FROM t"),
        expectedRows
      )
    }
  }

  test("NVL with GEOMETRY columns") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    // Test with columns, using geometries with different SRID values.
    val geom1 = "ST_GeomFromWKB(wkb)" // Column with fixed SRID (0).
    val geometryType1 = GeometryType(0)
    val geom2 = s"$geom1::GEOMETRY(ANY)" // Column with mixed SRID (ANY).
    val geometryType2 = GeometryType("ANY")
    val geo = "hex(ST_AsBinary(g)), ST_Srid(g)"
    val row = Row(wkbString, 0)

    val testCases = Seq(
      (s"nvl(null, $geom1)", geometryType1, Seq(row)),
      (s"nvl($geom1, null)", geometryType1, Seq(row)),
      (s"nvl(null, $geom2)", geometryType2, Seq(row)),
      (s"nvl($geom2, null)", geometryType2, Seq(row)),
      (s"nvl($geom1, $geom1)", geometryType1, Seq(row)),
      (s"nvl($geom2, $geom2)", geometryType2, Seq(row)),
      (s"nvl($geom1, $geom2)", mixedSridGeometryType, Seq(row)),
      (s"nvl($geom2, $geom1)", mixedSridGeometryType, Seq(row))
    )

    // Test with literal and column, using geometries with different SRID values.
    withTable("tbl") {
      // Construct and populate the test table.
      sql("CREATE TABLE tbl (wkb BINARY)")
      sql(s"INSERT INTO tbl VALUES (X'$wkbString')")

      for ((query, expectedType, expectedRows) <- testCases) {
        assertType(
          s"SELECT $query FROM tbl",
          expectedType
        )
        checkAnswer(
          sql(s"WITH t AS (SELECT $query AS g FROM tbl) SELECT $geo FROM t"),
          expectedRows
        )
      }
    }
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

  /** Geospatial feature is disabled. */

  test("verify that geospatial functions are disabled when the config is off") {
    withSQLConf(SQLConf.GEOSPATIAL_ENABLED.key -> "false") {
      val dummyArgument = "NULL"
      // Verify that SQL ST functions throw the expected exception.
      Seq(
        s"ST_AsBinary($dummyArgument)",
        s"ST_GeogFromWKB($dummyArgument)",
        s"ST_GeomFromWKB($dummyArgument)",
        s"ST_Srid($dummyArgument)",
        s"ST_SetSrid($dummyArgument, $dummyArgument)"
      ).foreach { query =>
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"SELECT $query").collect()
          },
          condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED"
        )
      }
    }
  }

  test("verify that geospatial type casting is disabled when the config is off") {
    withSQLConf(SQLConf.GEOSPATIAL_ENABLED.key -> "false") {
      // Verify that type casting with geospatial types throws the expected exception.
      Seq(
        "SELECT NULL::GEOGRAPHY(4326)",
        "SELECT NULL::GEOGRAPHY(ANY)",
        "SELECT NULL::GEOMETRY(4326)",
        "SELECT NULL::GEOMETRY(ANY)"
      ).foreach { query =>
        checkError(
          exception = intercept[AnalysisException] {
            sql(query).collect()
          },
          condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED")
      }
    }
  }

  test("verify that geospatial type coercion is disabled when the config is off") {
    withSQLConf(SQLConf.GEOSPATIAL_ENABLED.key -> "false") {
      // Verify that type coercion with geospatial types throws the expected exception.
      val value = "NULL"
      Seq(
        ("GEOGRAPHY(4326)", "GEOGRAPHY(ANY)"),
        ("GEOMETRY(4326)", "GEOMETRY(ANY)"),
        ("GEOMETRY(ANY)", "GEOMETRY(0)"),
        ("GEOMETRY(3857)", "GEOMETRY(4326)")
      ).foreach { case (type1, type2) =>
        val geo1 = s"CAST($value AS $type1)"
        val geo2 = s"CAST($value AS $type2)"
        Seq(
          s"SELECT array($geo1, $geo2)",
          s"SELECT nvl($geo1, $geo2)"
        ).foreach { query =>
          checkError(
            exception = intercept[AnalysisException] {
              sql(query).collect()
            },
            condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED")
        }
      }
    }
  }

}

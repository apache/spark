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

  /** ST writer expressions. */

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

  /** ST reader expressions. */

  test("ST_GeogFromWKB - expressions") {
    // Test data: WKB representation of POINT(1 2).
    val validWkb = Hex.unhex("0101000000000000000000F03F0000000000000040".getBytes())
    val validWkbLiteral = Literal.create(validWkb, BinaryType)
    val invalidWkb = Hex.unhex("010203".getBytes())
    val invalidWkbLiteral = Literal.create(invalidWkb, BinaryType)

    // ST_GeogFromWKB with valid WKB.
    val geogLitValidWkb = ST_GeogFromWKB(validWkbLiteral)
    assert(geogLitValidWkb.dataType.sameType(defaultGeographyType))
    checkEvaluation(ST_AsBinary(geogLitValidWkb), validWkb)
    // ST_GeogFromWKB with invalid WKB.
    val geogLitInvalidWkb = ST_GeogFromWKB(invalidWkbLiteral)
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        geogLitInvalidWkb.eval()
      },
      condition = "WKB_PARSE_ERROR",
      parameters = Map.empty
    )
  }

  test("ST_GeogFromWKB - table") {
    // Test data: WKB representation of POINT(1 2).
    val validWkbString = "0101000000000000000000F03F0000000000000040"
    val validWkb = Hex.unhex(validWkbString.getBytes())
    val invalidWkbString = "010203"

    withTable("tbl") {
      // Construct the test table.
      sql(s"CREATE TABLE tbl (validWkb BINARY, invalidWkb BINARY)")
      sql(s"INSERT INTO tbl VALUES (X'$validWkbString', X'$invalidWkbString')")

      // ST_GeogFromWKB with valid WKB column.
      val geogCol = "ST_GeogFromWKB(validWkb)"
      assertType(s"SELECT $geogCol FROM tbl", GeographyType(4326))
      checkAnswer(sql(s"SELECT ST_AsBinary($geogCol) FROM tbl"), Row(validWkb))
      // ST_GeogFromWKB with valid WKB literal.
      val geogLit = s"ST_GeogFromWKB(X'$validWkbString')"
      assertType(s"SELECT $geogLit", GeographyType(4326))
      checkAnswer(sql(s"SELECT ST_AsBinary($geogLit) AS wkb"), Row(validWkb))

      // ST_GeogFromWKB with invalid WKB column.
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          sql(s"SELECT ST_GeogFromWKB(invalidWkb) FROM tbl").collect()
        },
        condition = "WKB_PARSE_ERROR",
        parameters = Map.empty
      )
      // ST_GeogFromWKB with invalid WKB literal.
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          sql(s"SELECT ST_GeogFromWKB(X'$invalidWkbString')").collect()
        },
        condition = "WKB_PARSE_ERROR",
        parameters = Map.empty
      )
    }
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

}

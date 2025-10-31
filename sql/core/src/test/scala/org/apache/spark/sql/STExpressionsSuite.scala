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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class STExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  // Private common constants used across several tests.
  private val defaultGeographyType: DataType =
    GeographyType(ExpressionDefaults.DEFAULT_GEOGRAPHY_SRID)
  private val defaultGeometryType: DataType =
    GeometryType(ExpressionDefaults.DEFAULT_GEOMETRY_SRID)

  /** Casting geospatial data types. */

  test("Cast with GEOGRAPHY values") {
    // Test data: WKB representation of POINT(1 2).
    val wkbString = "0101000000000000000000F03F0000000000000040"
    val wkb = Hex.unhex(wkbString.getBytes())
    val wkbLiteral = Literal.create(wkb, BinaryType)

    // Construct the input expressions, using the specified WKB.
    val geographyExpression = ST_GeogFromWKB(wkbLiteral)
    // Execute expression-level unit tests and check data type.
    checkEvaluation(ST_AsBinary(geographyExpression), wkb)
    assert(geographyExpression.dataType.sameType(defaultGeographyType))
    // Cast the GEOGRAPHY with concrete SRID to a GEOGRAPHY type with mixed SRID.
    val castExpression = Cast(geographyExpression, GeographyType("ANY"))
    // Execute expression-level unit tests and check data type.
    checkEvaluation(ST_AsBinary(castExpression), wkb)
    assert(castExpression.dataType.sameType(GeographyType("ANY")))

    // Construct the input SQL query strings, using the specified WKB.
    val geogQuery: String = s"ST_GeogFromWKB(X'$wkbString')"
    // Execute end-to-end SQL query test and check data type.
    checkAnswer(sql(s"SELECT ST_AsBinary($geogQuery)"), Row(wkb))
    assert(sql(s"SELECT $geogQuery").schema.fields.head.dataType.sameType(defaultGeographyType))
    // Cast the GEOGRAPHY with concrete SRID to a GEOGRAPHY type with mixed SRID.
    val castQuery = s"$geogQuery::GEOGRAPHY(ANY)"
    // Execute end-to-end SQL query test and check data type.
    checkAnswer(sql(s"SELECT ST_AsBinary($castQuery)"), Row(wkb))
    assert(sql(s"SELECT $castQuery").schema.fields.head.dataType.sameType(GeographyType("ANY")))
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

}

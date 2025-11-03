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

}

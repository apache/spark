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

import scala.collection.immutable.Seq

import org.apache.spark.{SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.types._

class GeometryConnectDataFrameSuite extends QueryTest with RemoteSparkSession {

  private val point1: Array[Byte] = "010100000000000000000031400000000000001C40"
    .grouped(2)
    .map(Integer.parseInt(_, 16).toByte)
    .toArray
  private val point2: Array[Byte] = "010100000000000000000035400000000000001E40"
    .grouped(2)
    .map(Integer.parseInt(_, 16).toByte)
    .toArray

  test("decode geometry value: SRID schema does not match input SRID data schema") {
    val geometry = Geometry.fromWKB(point1, 4326)

    val seq = Seq((geometry, 1))
    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(seq).collect()
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOMETRY", "valueSrid" -> "4326", "typeSrid" -> "0"))

    import testImplicits._
    checkError(
      exception = intercept[SparkRuntimeException] {
        Seq(geometry).toDF().collect()
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOMETRY", "valueSrid" -> "4326", "typeSrid" -> "0"))
  }

  test("decode geometry value: mixed SRID schema is provided") {
    val schema = StructType(Seq(StructField("col1", GeometryType("ANY"), nullable = false)))
    val expectedResult =
      Seq(Row(Geometry.fromWKB(point1, 0)), Row(Geometry.fromWKB(point2, 4326)))

    val javaList = java.util.Arrays
      .asList(Row(Geometry.fromWKB(point1, 0)), Row(Geometry.fromWKB(point2, 4326)))
    val resultJavaListDF = spark.createDataFrame(javaList, schema)
    checkAnswer(resultJavaListDF, expectedResult)

    // Test that unsupported SRID with mixed schema will throw an error.
    val invalidData =
      java.util.Arrays
        .asList(Row(Geometry.fromWKB(point1, 1)), Row(Geometry.fromWKB(point2, 4326)))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        spark.createDataFrame(invalidData, schema).collect()
      },
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> "1"))
  }

  test("createDataFrame APIs with Geometry.fromWKB") {
    val geometry1 = Geometry.fromWKB(point1, 0)
    val geometry2 = Geometry.fromWKB(point2, 0)

    // 1. Test createDataFrame with Seq of Geometry objects
    val seq = Seq((geometry1, 1), (geometry2, 2), (null, 3))
    val dfFromSeq = spark.createDataFrame(seq)
    checkAnswer(dfFromSeq, Seq(Row(geometry1, 1), Row(geometry2, 2), Row(null, 3)))

    // 2. Test createDataFrame with RDD of Rows and StructType schema
    val geometry3 = Geometry.fromWKB(point1, 4326)
    val geometry4 = Geometry.fromWKB(point2, 4326)
    val schema = StructType(Seq(StructField("geometry", GeometryType(4326), nullable = true)))

    // 3. Test createDataFrame with Java List of Rows and StructType schema
    val javaList = java.util.Arrays.asList(Row(geometry3), Row(geometry4), Row(null))
    val dfFromJavaList = spark.createDataFrame(javaList, schema)
    checkAnswer(dfFromJavaList, Seq(Row(geometry3), Row(geometry4), Row(null)))

    // 4. Implicit conversion from Seq to DF
    import testImplicits._
    val implicitDf = Seq(geometry1, geometry2, null).toDF()
    checkAnswer(implicitDf, Seq(Row(geometry1), Row(geometry2), Row(null)))
  }

  test("encode geometry type") {
    // POINT (17 7)
    val wkb = "010100000000000000000031400000000000001C40"
    val df = spark.sql(s"SELECT ST_GeomFromWKB(X'$wkb')")
    val point = wkb.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
    val expectedGeom = Geometry.fromWKB(point, 0)
    checkAnswer(df, Seq(Row(expectedGeom)))
  }

  test("geospatial feature disabled") {
    withSQLConf("spark.sql.geospatial.enabled" -> "false") {
      val geometry = Geometry.fromWKB(point1, 0)
      val schema = StructType(Seq(StructField("col1", GeometryType(0))))
      // Java List[Row] + schema.
      val javaList = java.util.Arrays.asList(Row(geometry))
      checkError(
        exception = intercept[AnalysisException] {
          spark.createDataFrame(javaList, schema).collect()
        },
        condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED")
      // Implicit encoder path.
      import testImplicits._
      checkError(
        exception = intercept[AnalysisException] {
          Seq(geometry).toDF("g").collect()
        },
        condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED")
    }
  }
}

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

class GeographyConnectDataFrameSuite extends QueryTest with RemoteSparkSession {

  private val point1: Array[Byte] = "010100000000000000000031400000000000001C40"
    .grouped(2)
    .map(Integer.parseInt(_, 16).toByte)
    .toArray
  private val point2: Array[Byte] = "010100000000000000000035400000000000001E40"
    .grouped(2)
    .map(Integer.parseInt(_, 16).toByte)
    .toArray

  test("decode geography value: SRID schema does not match input SRID data schema") {
    val geography = Geography.fromWKB(point1, 0)

    val seq = Seq((geography, 1))
    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(seq).collect()
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "0", "typeSrid" -> "4326"))

    import testImplicits._
    checkError(
      exception = intercept[SparkRuntimeException] {
        Seq(geography).toDF().collect()
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "0", "typeSrid" -> "4326"))
  }

  test("decode geography value: mixed SRID schema is provided") {
    val schema = StructType(Seq(StructField("col1", GeographyType("ANY"), nullable = false)))
    val expectedResult =
      Seq(Row(Geography.fromWKB(point1, 4326)), Row(Geography.fromWKB(point2, 4326)))

    val javaList = java.util.Arrays
      .asList(Row(Geography.fromWKB(point1, 4326)), Row(Geography.fromWKB(point2, 4326)))
    val resultJavaListDF = spark.createDataFrame(javaList, schema)
    checkAnswer(resultJavaListDF, expectedResult)

    // Test that unsupported SRID with mixed schema will throw an error.
    val invalidData =
      java.util.Arrays
        .asList(Row(Geography.fromWKB(point1, 1)), Row(Geography.fromWKB(point2, 4326)))
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        spark.createDataFrame(invalidData, schema).collect()
      },
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> "1"))
  }

  test("createDataFrame APIs with Geography.fromWKB") {
    val geography1 = Geography.fromWKB(point1, 4326)
    val geography2 = Geography.fromWKB(point2)

    val seq = Seq((geography1, 1), (geography2, 2), (null, 3))
    val dfFromSeq = spark.createDataFrame(seq)
    checkAnswer(dfFromSeq, Seq(Row(geography1, 1), Row(geography2, 2), Row(null, 3)))

    val schema = StructType(Seq(StructField("geography", GeographyType(4326), nullable = true)))

    val javaList = java.util.Arrays.asList(Row(geography1), Row(geography2), Row(null))
    val dfFromJavaList = spark.createDataFrame(javaList, schema)
    checkAnswer(dfFromJavaList, Seq(Row(geography1), Row(geography2), Row(null)))

    import testImplicits._
    val implicitDf = Seq(geography1, geography2, null).toDF()
    checkAnswer(implicitDf, Seq(Row(geography1), Row(geography2), Row(null)))
  }

  test("encode geography type") {
    // POINT (17 7)
    val wkb = "010100000000000000000031400000000000001C40"
    val df = spark.sql(s"SELECT ST_GeogFromWKB(X'$wkb')")
    val point = wkb.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
    val expectedGeog = Geography.fromWKB(point, 4326)
    checkAnswer(df, Seq(Row(expectedGeog)))
  }

  test("geospatial feature disabled") {
    withSQLConf("spark.sql.geospatial.enabled" -> "false") {
      val geography = Geography.fromWKB(point1, 4326)
      val schema = StructType(Seq(StructField("col1", GeographyType(4326))))
      // Java List[Row] + schema.
      val javaList = java.util.Arrays.asList(Row(geography))
      checkError(
        exception = intercept[AnalysisException] {
          spark.createDataFrame(javaList, schema).collect()
        },
        condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED")
      // Implicit encoder path.
      import testImplicits._
      checkError(
        exception = intercept[AnalysisException] {
          Seq(geography).toDF("g").collect()
        },
        condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED")
    }
  }
}

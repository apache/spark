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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class GeographyDataFrameSuite extends QueryTest with SharedSparkSession {

  val point1 = "010100000000000000000031400000000000001C40"
    .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
  val point2 = "010100000000000000000035400000000000001E40"
    .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

  test("decode geography value: SRID schema does not match input SRID data schema") {
    val rdd = sparkContext.parallelize(Seq(Row(Geography.fromWKB(point1, 0))))
    val schema = StructType(Seq(StructField("col1", GeographyType(4326), nullable = false)))
    checkError(
      // We look for cause, as all exception encoder errors are wrapped in
      // EXPRESSION_ENCODING_FAILED.
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(rdd, schema).collect()
      }.getCause.asInstanceOf[SparkRuntimeException],
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "0", "typeSrid" -> "4326")
    )

    val javaRDD = sparkContext.parallelize(Seq(Row(Geography.fromWKB(point1, 0)))).toJavaRDD()
    checkError(
      // We look for cause, as all exception encoder errors are wrapped in
      // EXPRESSION_ENCODING_FAILED.
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(javaRDD, schema).collect()
      }.getCause.asInstanceOf[SparkRuntimeException],
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "0", "typeSrid" -> "4326")
    )

    // For some reason this API does not use expression encoders,
    // but CatalystTypeConverter, so we are not looking at cause.
    val javaList = java.util.Arrays.asList(Row(Geography.fromWKB(point1, 0)))
    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(javaList, schema).collect()
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "0", "typeSrid" -> "4326")
    )

    val geography1 = Geography.fromWKB(point1, 0)
    val rdd2 = sparkContext.parallelize(Seq((geography1, 1)))
    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(rdd2).collect()
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "0", "typeSrid" -> "4326")
    )

    // For some reason this API does not use expression encoders,
    // but CatalystTypeConverter, so we are not looking at cause.
    val seq = Seq((geography1, 1))
    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(seq).collect()
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "0", "typeSrid" -> "4326")
    )

    import testImplicits._
    checkError(
      exception = intercept[SparkRuntimeException] {
        Seq(geography1).toDF().collect()
      }.getCause.asInstanceOf[SparkRuntimeException],
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "0", "typeSrid" -> "4326")
    )
  }

  test("decode geography value: mixed SRID schema is provided") {
    val rdd = sparkContext.parallelize(
      Seq(Row(Geography.fromWKB(point1, 4326)), Row(Geography.fromWKB(point2, 4326))))
    val schema = StructType(Seq(StructField("col1", GeographyType("ANY"), nullable = false)))
    val expectedResult = Seq(
      Row(Geography.fromWKB(point1, 4326)), Row(Geography.fromWKB(point2, 4326)))

    val resultDF = spark.createDataFrame(rdd, schema)
    checkAnswer(resultDF, expectedResult)

    val javaRDD = sparkContext.parallelize(
      Seq(Row(Geography.fromWKB(point1, 4326)), Row(Geography.fromWKB(point2, 4326)))).toJavaRDD()
    val resultJavaDF = spark.createDataFrame(javaRDD, schema)
    checkAnswer(resultJavaDF, expectedResult)

    val javaList = java.util.Arrays.asList(
      Row(Geography.fromWKB(point1, 4326)), Row(Geography.fromWKB(point2, 4326)))
    val resultJavaListDF = spark.createDataFrame(javaList, schema)
    checkAnswer(resultJavaListDF, expectedResult)

    // Test that unsupported SRID with mixed schema will throw an error.
    val rdd2 = sparkContext.parallelize(
      Seq(Row(Geography.fromWKB(point1, 0)), Row(Geography.fromWKB(point2, 4326))))
    checkError(
      exception = intercept[SparkRuntimeException] {
        spark.createDataFrame(rdd2, schema).collect()
      }.getCause.asInstanceOf[SparkIllegalArgumentException],
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> "0")
    )
  }

  test("createDataFrame APIs with Geography.fromWKB") {
    // 1. Test createDataFrame with RDD of Geography objects
    val geography1 = Geography.fromWKB(point1, 4326)
    val geography2 = Geography.fromWKB(point2)
    val rdd = sparkContext.parallelize(Seq((geography1, 1), (geography2, 2), (null, 3)))
    val dfFromRDD = spark.createDataFrame(rdd)
    checkAnswer(dfFromRDD, Seq(Row(geography1, 1), Row(geography2, 2), Row(null, 3)))

    // 2. Test createDataFrame with Seq of Geography objects
    val seq = Seq((geography1, 1), (geography2, 2), (null, 3))
    val dfFromSeq = spark.createDataFrame(seq)
    checkAnswer(dfFromSeq, Seq(Row(geography1, 1), Row(geography2, 2), Row(null, 3)))

    // 3. Test createDataFrame with RDD of Rows and StructType schema
    val geography3 = Geography.fromWKB(point1, 4326)
    val geography4 = Geography.fromWKB(point2, 4326)
    val rowRDD = sparkContext.parallelize(Seq(Row(geography3), Row(geography4), Row(null)))
    val schema = StructType(Seq(
      StructField("geography", GeographyType(4326), nullable = true)
    ))
    val dfFromRowRDD = spark.createDataFrame(rowRDD, schema)
    checkAnswer(dfFromRowRDD, Seq(Row(geography3), Row(geography4), Row(null)))

    // 4. Test createDataFrame with JavaRDD of Rows and StructType schema
    val javaRDD = sparkContext.parallelize(Seq(Row(geography3), Row(geography4), Row(null)))
      .toJavaRDD()
    val dfFromJavaRDD = spark.createDataFrame(javaRDD, schema)
    checkAnswer(dfFromJavaRDD, Seq(Row(geography3), Row(geography4), Row(null)))

    // 5. Test createDataFrame with Java List of Rows and StructType schema
    val javaList = java.util.Arrays.asList(Row(geography3), Row(geography4), Row(null))
    val dfFromJavaList = spark.createDataFrame(javaList, schema)
    checkAnswer(dfFromJavaList, Seq(Row(geography3), Row(geography4), Row(null)))

    // 6. Implicit conversion from Seq to DF
    import testImplicits._
    val implicitDf = Seq(geography1, geography2, null).toDF()
    checkAnswer(implicitDf, Seq(Row(geography1), Row(geography2), Row(null)))
  }

  test("encode geography type") {
    // A test WKB value corresponding to: POINT (17 7).
    val pointString: String = "010100000000000000000031400000000000001C40"
    val pointBytes: Array[Byte] = pointString
      .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
    val df = spark.sql(s"SELECT ST_GeogFromWKB(X'$pointString')")
    val expectedGeog = Geography.fromWKB(pointBytes, 4326)
    checkAnswer(df, Seq(Row(expectedGeog)))
  }

  test("geospatial feature disabled") {
    withSQLConf(SQLConf.GEOSPATIAL_ENABLED.key -> "false") {
      val geography = Geography.fromWKB(point1, 4326)
      val schema = StructType(Seq(StructField("col1", GeographyType(4326))))
      // RDD[Row] + schema.
      val rdd = sparkContext.parallelize(Seq(Row(geography)))
      checkError(
        exception = intercept[AnalysisException] {
          spark.createDataFrame(rdd, schema).collect()
        },
        condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED"
      )
      // Java List[Row] + schema.
      val javaList = java.util.Arrays.asList(Row(geography))
      checkError(
        exception = intercept[AnalysisException] {
          spark.createDataFrame(javaList, schema).collect()
        },
        condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED"
      )
      // Implicit encoder path.
      import testImplicits._
      checkError(
        exception = intercept[AnalysisException] {
          Seq(geography).toDF("g").collect()
        },
        condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED"
      )
    }
  }
}

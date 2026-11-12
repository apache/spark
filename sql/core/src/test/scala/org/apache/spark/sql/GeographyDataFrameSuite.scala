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
import org.apache.spark.sql.catalyst.util.STUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.BinaryView

class GeographyDataFrameSuite extends SharedSparkSession {

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

  test("createDataFrame and round-trip with Geography SRIDs") {
    Seq(4267, 4269).foreach { srid =>
      val geog = Geography.fromWKB(point1, srid)
      val schema = StructType(Seq(StructField("g", GeographyType(srid), nullable = false)))
      checkAnswer(
        spark.createDataFrame(sparkContext.parallelize(Seq(Row(geog))), schema),
        Seq(Row(geog)))
    }
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

  test("SPARK-57058: ORDER BY on a Geography column fails with INVALID_ORDERING_TYPE") {
    val rdd = sparkContext.parallelize(Seq(
      Row(Geography.fromWKB(point1, 4326)),
      Row(Geography.fromWKB(point2, 4326))))
    val schema = StructType(Seq(StructField("g", GeographyType(4326), nullable = false)))
    spark.createDataFrame(rdd, schema).createOrReplaceTempView("geog_t")
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql("SELECT g FROM geog_t ORDER BY g").collect()
      },
      condition = "DATATYPE_MISMATCH.INVALID_ORDERING_TYPE",
      parameters = Map(
        "functionName" -> "`sortorder`",
        "dataType" -> "\"GEOGRAPHY(4326)\"",
        "sqlExpr" -> "\"g ASC NULLS FIRST\""),
      queryContext = Array(ExpectedContext("g", 30, 30)))
  }

  test("SPARK-61081: GROUP BY, DISTINCT and hashing on a Geography column") {
    // The GROUP BY, DISTINCT and DISTRIBUTE BY queries below hash-partition on `g` in a shuffle.
    // With four input partitions, equal values reach that shuffle from different map tasks, so
    // the test checks that their hashes agree.
    val rdd = sparkContext.parallelize(
      Seq.tabulate(20)(i => Row(Geography.fromWKB(if (i % 4 == 0) point2 else point1, 4326))), 4)
    val schema = StructType(Seq(StructField("g", GeographyType(4326), nullable = false)))
    spark.createDataFrame(rdd, schema).createOrReplaceTempView("geog_t")

    val groupBy = "SELECT COUNT(*) FROM geog_t GROUP BY g"
    checkAnswer(spark.sql(groupBy), Seq(Row(5L), Row(15L)))
    checkAnswer(spark.sql("SELECT COUNT(DISTINCT g) FROM geog_t"), Row(2L))
    checkAnswer(spark.sql("SELECT COUNT(*) FROM (SELECT DISTINCT g FROM geog_t)"), Row(2L))
    checkAnswer(spark.sql("SELECT approx_count_distinct(g) FROM geog_t"), Row(2L))
    checkAnswer(
      spark.sql("SELECT COUNT(*) FROM (SELECT * FROM geog_t DISTRIBUTE BY g)"), Row(20L))
    checkAnswer(
      spark.sql("SELECT COUNT(DISTINCT hash(g)), COUNT(DISTINCT xxhash64(g)) FROM geog_t"),
      Row(2L, 2L))

    // Forces the hash aggregate to spill into its sort-based fallback, which orders the keys.
    withSQLConf("spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1,2") {
      checkAnswer(spark.sql(groupBy), Seq(Row(5L), Row(15L)))
    }
  }

  test("SPARK-61081: bucketed writes of non-NULL Geography values fail") {
    // POINT(1 2) as little-endian and as big-endian WKB. Parquet stores both as little-endian
    // WKB, so a bucket id computed from the big-endian bytes would not match the stored bytes.
    val littleEndian = "X'0101000000000000000000F03F0000000000000040'"
    val bigEndian = "X'00000000013FF00000000000004000000000000000'"
    val values = s"SELECT ST_GeogFromWKB($littleEndian) AS g " +
      s"UNION ALL SELECT ST_GeogFromWKB($bigEndian) AS g"
    val nullValue = "ST_GeogFromWKB(CAST(NULL AS BINARY))"
    def checkBucketedWriteFails(write: => Any, dataType: String = "\"GEOGRAPHY(4326)\""): Unit = {
      checkError(
        exception = intercept[SparkRuntimeException](write),
        condition = "INVALID_BUCKET_COLUMN_DATA_TYPE",
        parameters = Map("type" -> dataType))
    }

    withTable("t", "t_ctas", "t_df", "t_struct", "t_array", "t_map", "t_map_key", "t_udt",
        "t_udt_struct", "t_other") {
      sql("CREATE TABLE t (g GEOGRAPHY(4326)) USING parquet CLUSTERED BY (g) INTO 8 BUCKETS")
      Seq("true", "false").foreach { plannedWrite =>
        withSQLConf(SQLConf.PLANNED_WRITE_ENABLED.key -> plannedWrite) {
          checkBucketedWriteFails(sql(s"INSERT INTO t $values"))
          // A NULL value hashes to the same bucket before and after the Parquet round trip.
          sql(s"INSERT INTO t SELECT $nullValue")
        }
      }
      checkAnswer(spark.table("t"), Seq(Row(null), Row(null)))

      checkBucketedWriteFails(
        sql(s"CREATE TABLE t_ctas USING parquet CLUSTERED BY (g) INTO 8 BUCKETS AS $values"))
      assert(!spark.catalog.tableExists("t_ctas"))
      sql("CREATE TABLE t_ctas USING parquet CLUSTERED BY (g) " +
        s"INTO 8 BUCKETS AS SELECT * FROM ($values) WHERE false")
      checkAnswer(spark.table("t_ctas"), Seq.empty)
      checkBucketedWriteFails(sql(values).write.bucketBy(8, "g").saveAsTable("t_df"))
      assert(!spark.catalog.tableExists("t_df"))
      sql(values).limit(0).write.bucketBy(8, "g").saveAsTable("t_df")
      checkAnswer(spark.table("t_df"), Seq.empty)

      // In the nested cases, the non-NULL value follows a non-geo field or a NULL element.
      sql("CREATE TABLE t_struct (s STRUCT<id: INT, g: GEOGRAPHY(4326)>) USING parquet " +
        "CLUSTERED BY (s) INTO 8 BUCKETS")
      checkBucketedWriteFails(
        sql(s"INSERT INTO t_struct SELECT named_struct('id', 1, 'g', g) FROM ($values)"),
        "\"STRUCT<id: INT NOT NULL, g: GEOGRAPHY(4326) NOT NULL>\"")
      sql(s"INSERT INTO t_struct SELECT named_struct('id', 1, 'g', $nullValue)")
      checkAnswer(spark.table("t_struct"), Row(Row(1, null)))
      sql("CREATE TABLE t_array (a ARRAY<GEOGRAPHY(4326)>) USING parquet " +
        "CLUSTERED BY (a) INTO 8 BUCKETS")
      checkBucketedWriteFails(
        sql(s"INSERT INTO t_array SELECT array($nullValue, g) FROM ($values)"),
        "\"ARRAY<GEOGRAPHY(4326)>\"")
      sql(s"INSERT INTO t_array SELECT array($nullValue)")
      checkAnswer(spark.table("t_array"), Row(Seq(null)))
      // With planned writes on, the sort that V1Writes adds for a MAP bucket column does not
      // resolve, because the hash expression does not accept maps.
      withSQLConf(SQLConf.PLANNED_WRITE_ENABLED.key -> "false") {
        sql("CREATE TABLE t_map (m MAP<INT, GEOGRAPHY(4326)>) USING parquet " +
          "CLUSTERED BY (m) INTO 8 BUCKETS")
        checkBucketedWriteFails(
          sql(s"INSERT INTO t_map SELECT map(1, g) FROM ($values)"),
          "\"MAP<INT, GEOGRAPHY(4326)>\"")
        sql(s"INSERT INTO t_map SELECT map(1, $nullValue)")
        checkAnswer(spark.table("t_map"), Row(Map(1 -> null)))
        sql("CREATE TABLE t_map_key (m MAP<GEOGRAPHY(4326), INT>) USING parquet " +
          "CLUSTERED BY (m) INTO 8 BUCKETS")
        checkBucketedWriteFails(
          sql(s"INSERT INTO t_map_key SELECT map(g, 1) FROM ($values)"),
          "\"MAP<GEOGRAPHY(4326), INT>\"")
      }

      // The bucket id hash looks through a user-defined type to the geo value.
      def withUdt(query: String, dataType: DataType): DataFrame = spark.internalCreateDataFrame(
        sql(query).queryExecution.toRdd, new StructType().add("u", dataType))
      val udt = new TestGeographyUDT
      val udtType = "UDT(\"GEOGRAPHY(4326)\")"
      checkBucketedWriteFails(
        withUdt(values, udt).write.bucketBy(8, "u").saveAsTable("t_udt"), udtType)
      assert(!spark.catalog.tableExists("t_udt"))
      withUdt(s"SELECT $nullValue", udt).write.bucketBy(8, "u").saveAsTable("t_udt")
      checkBucketedWriteFails(withUdt(values, udt).write.insertInto("t_udt"), udtType)
      checkAnswer(spark.table("t_udt"), Row(null))
      checkBucketedWriteFails(
        withUdt(s"SELECT named_struct('g', g) FROM ($values)", new StructType().add("g", udt))
          .write.bucketBy(8, "u").saveAsTable("t_udt_struct"),
        "\"STRUCT<g: GEOGRAPHY(4326)>\"")
      assert(!spark.catalog.tableExists("t_udt_struct"))

      // Only bucket columns are checked.
      sql("CREATE TABLE t_other (id INT, g GEOGRAPHY(4326)) USING parquet " +
        "CLUSTERED BY (id) INTO 8 BUCKETS")
      sql(s"INSERT INTO t_other SELECT 1, g FROM ($values)")
      checkAnswer(spark.sql("SELECT COUNT(*) FROM t_other"), Row(2L))
    }
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

// A user-defined type whose values are GEOGRAPHY(4326) values.
private[sql] class TestGeographyUDT extends UserDefinedType[Geography] {
  override def sqlType: DataType = GeographyType(4326)
  override def serialize(obj: Geography): Any =
    STUtils.serializeGeogFromWKB(obj, GeographyType(4326))
  override def deserialize(datum: Any): Geography =
    STUtils.deserializeGeog(datum.asInstanceOf[BinaryView], GeographyType(4326))
  override def userClass: Class[Geography] = classOf[Geography]
}

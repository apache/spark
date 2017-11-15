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

package org.apache.spark.sql.hive.orc

import java.io.File
import java.util.{Locale, Properties}

import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcOutputFormat, OrcStruct}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.orc.OrcConf.COMPRESS
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.orc.OrcOptions
import org.apache.spark.sql.hive.HadoopTableReader
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.{SerializableConfiguration, Utils}

case class OrcData(intField: Int, stringField: String)

abstract class OrcSuite extends QueryTest with TestHiveSingleton with BeforeAndAfterAll {
  import spark._

  var orcTableDir: File = null
  var orcTableAsDir: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    orcTableAsDir = Utils.createTempDir("orctests", "sparksql")

    // Hack: to prepare orc data files using hive external tables
    orcTableDir = Utils.createTempDir("orctests", "sparksql")
    import org.apache.spark.sql.hive.test.TestHive.implicits._

    sparkContext
      .makeRDD(1 to 10)
      .map(i => OrcData(i, s"part-$i"))
      .toDF()
      .createOrReplaceTempView(s"orc_temp_table")

    sql(
      s"""CREATE EXTERNAL TABLE normal_orc(
         |  intField INT,
         |  stringField STRING
         |)
         |STORED AS ORC
         |LOCATION '${orcTableAsDir.toURI}'
       """.stripMargin)

    sql(
      s"""INSERT INTO TABLE normal_orc
         |SELECT intField, stringField FROM orc_temp_table
       """.stripMargin)
  }

  test("create temporary orc table") {
    checkAnswer(sql("SELECT COUNT(*) FROM normal_orc_source"), Row(10))

    checkAnswer(
      sql("SELECT * FROM normal_orc_source"),
      (1 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      sql("SELECT * FROM normal_orc_source where intField > 5"),
      (6 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      sql("SELECT COUNT(intField), stringField FROM normal_orc_source GROUP BY stringField"),
      (1 to 10).map(i => Row(1, s"part-$i")))
  }

  test("create temporary orc table as") {
    checkAnswer(sql("SELECT COUNT(*) FROM normal_orc_as_source"), Row(10))

    checkAnswer(
      sql("SELECT * FROM normal_orc_source"),
      (1 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      sql("SELECT * FROM normal_orc_source WHERE intField > 5"),
      (6 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      sql("SELECT COUNT(intField), stringField FROM normal_orc_source GROUP BY stringField"),
      (1 to 10).map(i => Row(1, s"part-$i")))
  }

  test("appending insert") {
    sql("INSERT INTO TABLE normal_orc_source SELECT * FROM orc_temp_table WHERE intField > 5")

    checkAnswer(
      sql("SELECT * FROM normal_orc_source"),
      (1 to 5).map(i => Row(i, s"part-$i")) ++ (6 to 10).flatMap { i =>
        Seq.fill(2)(Row(i, s"part-$i"))
      })
  }

  test("overwrite insert") {
    sql(
      """INSERT OVERWRITE TABLE normal_orc_as_source
        |SELECT * FROM orc_temp_table WHERE intField > 5
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM normal_orc_as_source"),
      (6 to 10).map(i => Row(i, s"part-$i")))
  }

  test("write null values") {
    sql("DROP TABLE IF EXISTS orcNullValues")

    val df = sql(
      """
        |SELECT
        |  CAST(null as TINYINT) as c0,
        |  CAST(null as SMALLINT) as c1,
        |  CAST(null as INT) as c2,
        |  CAST(null as BIGINT) as c3,
        |  CAST(null as FLOAT) as c4,
        |  CAST(null as DOUBLE) as c5,
        |  CAST(null as DECIMAL(7,2)) as c6,
        |  CAST(null as TIMESTAMP) as c7,
        |  CAST(null as DATE) as c8,
        |  CAST(null as STRING) as c9,
        |  CAST(null as VARCHAR(10)) as c10
        |FROM orc_temp_table limit 1
      """.stripMargin)

    df.write.format("orc").saveAsTable("orcNullValues")

    checkAnswer(
      sql("SELECT * FROM orcNullValues"),
      Row.fromSeq(Seq.fill(11)(null)))

    sql("DROP TABLE IF EXISTS orcNullValues")
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val conf = sqlContext.sessionState.conf
    val option = new OrcOptions(Map(COMPRESS.getAttribute.toUpperCase(Locale.ROOT) -> "NONE"), conf)
    assert(option.compressionCodec == "NONE")
  }

  test("SPARK-19459/SPARK-18220: read char/varchar column written by Hive") {
    val location = Utils.createTempDir()
    val uri = location.toURI
    try {
      hiveClient.runSqlHive("USE default")
      hiveClient.runSqlHive(
        """
          |CREATE EXTERNAL TABLE hive_orc(
          |  a STRING,
          |  b CHAR(10),
          |  c VARCHAR(10),
          |  d ARRAY<CHAR(3)>)
          |STORED AS orc""".stripMargin)
      // Hive throws an exception if I assign the location in the create table statement.
      hiveClient.runSqlHive(
        s"ALTER TABLE hive_orc SET LOCATION '$uri'")
      hiveClient.runSqlHive(
        """
          |INSERT INTO TABLE hive_orc
          |SELECT 'a', 'b', 'c', ARRAY(CAST('d' AS CHAR(3)))
          |FROM (SELECT 1) t""".stripMargin)

      // We create a different table in Spark using the same schema which points to
      // the same location.
      spark.sql(
        s"""
           |CREATE EXTERNAL TABLE spark_orc(
           |  a STRING,
           |  b CHAR(10),
           |  c VARCHAR(10),
           |  d ARRAY<CHAR(3)>)
           |STORED AS orc
           |LOCATION '$uri'""".stripMargin)
      val result = Row("a", "b         ", "c", Seq("d  "))
      checkAnswer(spark.table("hive_orc"), result)
      checkAnswer(spark.table("spark_orc"), result)
    } finally {
      hiveClient.runSqlHive("DROP TABLE IF EXISTS hive_orc")
      hiveClient.runSqlHive("DROP TABLE IF EXISTS spark_orc")
      Utils.deleteRecursively(location)
    }
  }

  test("SPARK-21839: Add SQL config for ORC compression") {
    val conf = sqlContext.sessionState.conf
    // Test if the default of spark.sql.orc.compression.codec is snappy
    assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec == "SNAPPY")

    // OrcOptions's parameters have a higher priority than SQL configuration.
    // `compression` -> `orc.compression` -> `spark.sql.orc.compression.codec`
    withSQLConf(SQLConf.ORC_COMPRESSION.key -> "uncompressed") {
      assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec == "NONE")
      val map1 = Map(COMPRESS.getAttribute -> "zlib")
      val map2 = Map(COMPRESS.getAttribute -> "zlib", "compression" -> "lzo")
      assert(new OrcOptions(map1, conf).compressionCodec == "ZLIB")
      assert(new OrcOptions(map2, conf).compressionCodec == "LZO")
    }

    // Test all the valid options of spark.sql.orc.compression.codec
    Seq("NONE", "UNCOMPRESSED", "SNAPPY", "ZLIB", "LZO").foreach { c =>
      withSQLConf(SQLConf.ORC_COMPRESSION.key -> c) {
        val expected = if (c == "UNCOMPRESSED") "NONE" else c
        assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec == expected)
      }
    }
  }
}

class OrcSourceSuite extends OrcSuite with SQLTestUtils with Matchers {
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql(
      s"""CREATE TEMPORARY VIEW normal_orc_source
         |USING org.apache.spark.sql.hive.orc
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).toURI}'
         |)
       """.stripMargin)

    spark.sql(
      s"""CREATE TEMPORARY VIEW normal_orc_as_source
         |USING org.apache.spark.sql.hive.orc
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).toURI}'
         |)
       """.stripMargin)
  }

  test("SPARK-12218 Converting conjunctions into ORC SearchArguments") {
    // The `LessThan` should be converted while the `StringContains` shouldn't
    val schema = new StructType(
      Array(
        StructField("a", IntegerType, nullable = true),
        StructField("b", StringType, nullable = true)))
    assertResult(
      """leaf-0 = (LESS_THAN a 10)
        |expr = leaf-0
      """.stripMargin.trim
    ) {
      OrcFilters.createFilter(schema, Array(
        LessThan("a", 10),
        StringContains("b", "prefix")
      )).get.toString
    }

    // The `LessThan` should be converted while the whole inner `And` shouldn't
    assertResult(
      """leaf-0 = (LESS_THAN a 10)
        |expr = leaf-0
      """.stripMargin.trim
    ) {
      OrcFilters.createFilter(schema, Array(
        LessThan("a", 10),
        Not(And(
          GreaterThan("a", 1),
          StringContains("b", "prefix")
        ))
      )).get.toString
    }
  }

  test("SPARK-22267 Spark SQL correctly reads parquet files when column order is different") {
    checkReadDataFrameFromFile("parquet", spark.read.parquet,
      HiveUtils.CONVERT_METASTORE_PARQUET.key)
  }

  test("SPARK-22267 Spark SQL incorrectly reads ORC files when column order is different") {
    checkReadDataFrameFromFile("orc", spark.read.orc, HiveUtils.CONVERT_METASTORE_ORC.key)
  }

  def checkReadDataFrameFromFile(format: String, read: String => DataFrame,
                                 convertConfigKey: String): Unit = {
    withTempDir { dir =>
      import spark.implicits._

      val path = dir.getCanonicalPath

      Seq(1 -> 2).toDF("c1", "c2").write.format(format).mode("overwrite").save(path)
      checkAnswer(read(path), Row(1, 2))

      Seq("true", "false").foreach { value =>
        withTable("t") {
          withSQLConf(convertConfigKey -> value) {
            sql(s"CREATE EXTERNAL TABLE t(c2 INT, c1 INT) STORED AS $format LOCATION '$path'")
            checkAnswer(spark.table("t"), Row(2, 1))
          }
        }
      }
    }
  }

  test("SPARK-22267 HadoopRDD incorrectly reads ORC files when column order is different") {
    for {
      serializationDdl <- Seq("struct t { i32 c2, i32 c1}", "struct t { i32 c1, i32 c2}")
      (columns, expected) <- Seq("c2,c1" -> "{2, 1}", "c1,c2" -> "{1, 2}")
    } yield {
      readOrcFile(serializationDdl, columns) shouldBe expected
    }
  }

  def readOrcFile(serializationDdl: String, columns: String): String = {
    withTempDir { dir =>
      import spark.implicits._
      val path = dir.getCanonicalPath
      Seq(1 -> 2).toDF("c1", "c2").write.format("orc").mode("overwrite").save(path)

      val properties = new Properties
      // scalastyle:off ensure.single.space.before.token
      Map(
        "name"                  -> "default.t",
        "columns.types"         -> "int:int",
        "serialization.ddl"     -> serializationDdl,
        "serialization.format"  -> "1",
        "columns"               -> columns,
        "columns.comments"      -> "",
        "bucket_count"          -> "-1",
        "EXTERNAL"              -> "TRUE",
        "serialization.lib"     -> "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        "file.inputformat"      -> "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "file.outputformat"     -> "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
        "location"              -> path,
        "transient_lastDdlTime" -> "1510594468"
      ).foreach(Function.tupled(properties.setProperty))
      // scalastyle:on ensure.single.space.before.token

      val (inputFormatClass, outputFormatClass) =
        (classOf[OrcInputFormat], classOf[OrcOutputFormat])
      val tableDesc1 = new TableDesc(inputFormatClass, outputFormatClass, properties)

      val initializeJobConfFunc = { jobConf: JobConf =>
        HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc1)(jobConf)
      }

      val broadCastedConf =
        sparkContext.broadcast(new SerializableConfiguration(sparkContext.hadoopConfiguration))

      val hadoopRDD = new HadoopRDD[NullWritable, OrcStruct](
        sparkContext,
        broadCastedConf,
        Some(initializeJobConfFunc),
        inputFormatClass,
        classOf[NullWritable],
        classOf[OrcStruct],
        0)

      hadoopRDD.map(_._2).map(_.toString).collect().mkString(", ")
    }
  }

}

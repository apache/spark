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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

case class OrcData(intField: Int, stringField: String)

abstract class OrcSuite extends QueryTest
    with TestHiveSingleton with SQLTestUtils with BeforeAndAfterAll {
  import spark.implicits._

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
         |LOCATION '${orcTableAsDir.getCanonicalPath}'
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

  test("prevent all column partitioning") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val e = intercept[AnalysisException] {
        spark.range(10).write.format("orc").mode("overwrite").partitionBy("id").save(path)
      }.getMessage
      assert(e.contains("Cannot use all columns for partition columns"))
    }
  }

  test("save API - empty path or illegal path") {
    var e = intercept[IllegalArgumentException] {
      spark.range(1).coalesce(1).write.format("orc").save()
    }.getMessage
    assert(e.contains("'path' is not specified"))

    e = intercept[IllegalArgumentException] {
      spark.range(1).coalesce(1).write.orc("")
    }.getMessage
    assert(e.contains("Can not create a Path from an empty string"))
  }

  test("load API - empty path") {
    val expectedErrorMsg = "'path' is not specified"
    var e = intercept[IllegalArgumentException] {
      spark.read.orc()
    }.getMessage
    assert(e.contains(expectedErrorMsg))

    e = intercept[IllegalArgumentException] {
      spark.read.format("orc").load().show()
    }.getMessage
    assert(e.contains(expectedErrorMsg))
  }

  test("illegal compression") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(0, 10)
      val e = intercept[IllegalArgumentException] {
        df.write.option("compression", "illegal").mode("overwrite").format("orc").save(path)
      }.getMessage
      assert(e.contains("Codec [illegal] is not available. Known codecs are"))
    }
  }

  test("orc - API") {
    val userSchema = new StructType().add("s", StringType)
    val data = Seq("1", "2", "3")
    val dir = Utils.createTempDir(namePrefix = "input").getCanonicalPath

    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).orc(dir)
    val df = spark.read.orc(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    intercept[IllegalArgumentException] {
      testRead(spark.read.orc(), Seq.empty, schema)
    }
    testRead(spark.read.orc(dir), data, schema)
    testRead(spark.read.orc(dir, dir), data ++ data, schema)
    testRead(spark.read.orc(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.orc).get, data, schema)

    // Reader, with user specified schema, report an exception as schema in file different
    // from user schema.
    testRead(spark.read.schema(userSchema).orc(), Seq.empty, userSchema)
    var e = intercept[SparkException] {
      testRead(spark.read.schema(userSchema).orc(dir), Seq.empty, userSchema)
    }.getMessage
    assert(e.contains("Field \"s\" does not exist"))
    e = intercept[SparkException] {
      testRead(spark.read.schema(userSchema).orc(dir, dir), Seq.empty, userSchema)
    }.getMessage
    assert(e.contains("Field \"s\" does not exist"))
    e = intercept[SparkException] {
      testRead(spark.read.schema(userSchema).orc(Seq(dir, dir): _*), Seq.empty, userSchema)
    }.getMessage
    assert(e.contains("Field \"s\" does not exist"))
  }

  private def testRead(
      df: => DataFrame,
      expectedResult: Seq[String],
      expectedSchema: StructType): Unit = {
    checkAnswer(df, spark.createDataset(expectedResult).toDF())
    assert(df.schema === expectedSchema)
  }
}

class OrcSourceSuite extends OrcSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sql(
      s"""CREATE TEMPORARY TABLE normal_orc_source
         |USING org.apache.spark.sql.hive.orc
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).getCanonicalPath}'
         |)
       """.stripMargin)

    spark.sql(
      s"""CREATE TEMPORARY TABLE normal_orc_as_source
         |USING org.apache.spark.sql.hive.orc
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).getCanonicalPath}'
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
}

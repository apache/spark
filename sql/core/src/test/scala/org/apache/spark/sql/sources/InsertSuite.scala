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

package org.apache.spark.sql.sources

import java.io.File

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class InsertSuite extends DataSourceTest with SharedSQLContext {
  protected override lazy val sql = spark.sql _
  private var path: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    path = Utils.createTempDir()
    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str$i"}"""))
    spark.read.json(rdd).createOrReplaceTempView("jt")
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable (a int, b string)
        |USING org.apache.spark.sql.json.DefaultSource
        |OPTIONS (
        |  path '${path.toString}'
        |)
      """.stripMargin)
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("jsonTable")
      spark.catalog.dropTempView("jt")
      Utils.deleteRecursively(path)
    } finally {
      super.afterAll()
    }
  }

  test("Simple INSERT OVERWRITE a JSONRelation") {
    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )
  }

  test("insert into a temp view that does not point to an insertable data source") {
    import testImplicits._
    withTempView("t1", "t2") {
      sql(
        """
          |CREATE TEMPORARY VIEW t1
          |USING org.apache.spark.sql.sources.SimpleScanSource
          |OPTIONS (
          |  From '1',
          |  To '10')
        """.stripMargin)
      sparkContext.parallelize(1 to 10).toDF("a").createOrReplaceTempView("t2")

      val message = intercept[AnalysisException] {
        sql("INSERT INTO TABLE t1 SELECT a FROM t2")
      }.getMessage
      assert(message.contains("does not allow insertion"))
    }
  }

  test("PreInsert casting and renaming") {
    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a * 2, a * 4 FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i * 2, s"${i * 4}"))
    )

    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a * 4 AS A, a * 6 as c FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i * 4, s"${i * 6}"))
    )
  }

  test("SELECT clause generating a different number of columns is not allowed.") {
    val message = intercept[AnalysisException] {
      sql(
        s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a FROM jt
      """.stripMargin)
    }.getMessage
    assert(message.contains("the number of columns are different")
    )
  }

  test("INSERT OVERWRITE a JSONRelation multiple times") {
    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )

    // Writing the table to less part files.
    val rdd1 = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str$i"}"""), 5)
    spark.read.json(rdd1).createOrReplaceTempView("jt1")
    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt1
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )

    // Writing the table to more part files.
    val rdd2 = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str$i"}"""), 10)
    spark.read.json(rdd2).createOrReplaceTempView("jt2")
    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt2
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i"))
    )

    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a * 10, b FROM jt1
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      (1 to 10).map(i => Row(i * 10, s"str$i"))
    )

    spark.catalog.dropTempView("jt1")
    spark.catalog.dropTempView("jt2")
  }

  test("INSERT INTO JSONRelation for now") {
    sql(
      s"""
      |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt").collect()
    )

    sql(
      s"""
         |INSERT INTO TABLE jsonTable SELECT a, b FROM jt
    """.stripMargin)
    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt UNION ALL SELECT a, b FROM jt").collect()
    )
  }

  test("it is not allowed to write to a table while querying it.") {
    val message = intercept[AnalysisException] {
      sql(
        s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jsonTable
      """.stripMargin)
    }.getMessage
    assert(
      message.contains("Cannot overwrite a path that is also being read from."),
      "INSERT OVERWRITE to a table while querying it should not be allowed.")
  }

  test("Caching")  {
    // write something to the jsonTable
    sql(
      s"""
         |INSERT OVERWRITE TABLE jsonTable SELECT a, b FROM jt
      """.stripMargin)
    // Cached Query Execution
    spark.catalog.cacheTable("jsonTable")
    assertCached(sql("SELECT * FROM jsonTable"))
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      (1 to 10).map(i => Row(i, s"str$i")))

    assertCached(sql("SELECT a FROM jsonTable"))
    checkAnswer(
      sql("SELECT a FROM jsonTable"),
      (1 to 10).map(Row(_)).toSeq)

    assertCached(sql("SELECT a FROM jsonTable WHERE a < 5"))
    checkAnswer(
      sql("SELECT a FROM jsonTable WHERE a < 5"),
      (1 to 4).map(Row(_)).toSeq)

    assertCached(sql("SELECT a * 2 FROM jsonTable"))
    checkAnswer(
      sql("SELECT a * 2 FROM jsonTable"),
      (1 to 10).map(i => Row(i * 2)).toSeq)

    assertCached(sql(
      "SELECT x.a, y.a FROM jsonTable x JOIN jsonTable y ON x.a = y.a + 1"), 2)
    checkAnswer(sql(
      "SELECT x.a, y.a FROM jsonTable x JOIN jsonTable y ON x.a = y.a + 1"),
      (2 to 10).map(i => Row(i, i - 1)).toSeq)

    // Insert overwrite and keep the same schema.
    sql(
      s"""
        |INSERT OVERWRITE TABLE jsonTable SELECT a * 2, b FROM jt
      """.stripMargin)
    // jsonTable should be recached.
    assertCached(sql("SELECT * FROM jsonTable"))
    // TODO we need to invalidate the cached data in InsertIntoHadoopFsRelation
//    // The cached data is the new data.
//    checkAnswer(
//      sql("SELECT a, b FROM jsonTable"),
//      sql("SELECT a * 2, b FROM jt").collect())
//
//    // Verify uncaching
//    spark.catalog.uncacheTable("jsonTable")
//    assertCached(sql("SELECT * FROM jsonTable"), 0)
  }

  test("it's not allowed to insert into a relation that is not an InsertableRelation") {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTen
        |USING org.apache.spark.sql.sources.SimpleScanSource
        |OPTIONS (
        |  From '1',
        |  To '10'
        |)
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM oneToTen"),
      (1 to 10).map(Row(_)).toSeq
    )

    val message = intercept[AnalysisException] {
      sql(
        s"""
        |INSERT OVERWRITE TABLE oneToTen SELECT CAST(a AS INT) FROM jt
        """.stripMargin)
    }.getMessage
    assert(
      message.contains("does not allow insertion."),
      "It is not allowed to insert into a table that is not an InsertableRelation."
    )

    spark.catalog.dropTempView("oneToTen")
  }

  test("SPARK-15824 - Execute an INSERT wrapped in a WITH statement immediately") {
    withTable("target", "target2") {
      sql(s"CREATE TABLE target(a INT, b STRING) USING JSON")
      sql("WITH tbl AS (SELECT * FROM jt) INSERT OVERWRITE TABLE target SELECT a, b FROM tbl")
      checkAnswer(
        sql("SELECT a, b FROM target"),
        sql("SELECT a, b FROM jt")
      )

      sql(s"CREATE TABLE target2(a INT, b STRING) USING JSON")
      val e = sql(
        """
          |WITH tbl AS (SELECT * FROM jt)
          |FROM tbl
          |INSERT INTO target2 SELECT a, b WHERE a <= 5
          |INSERT INTO target2 SELECT a, b WHERE a > 5
        """.stripMargin)
      checkAnswer(
        sql("SELECT a, b FROM target2"),
        sql("SELECT a, b FROM jt")
      )
    }
  }
}

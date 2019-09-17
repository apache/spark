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

package org.apache.spark.sql.connector

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, LiteralValue}
import org.apache.spark.sql.types.IntegerType

class DataSourceV2DataFrameSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = false) {
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    InMemoryV1Provider.clear()
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
    InMemoryV1Provider.clear()
  }

  override protected val catalogAndNamespace: String = "testcat.ns1.ns2.tbls"
  override protected val v2Format: String = classOf[FakeV2Provider].getName

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    dfw.insertInto(tableName)
  }

  test("insertInto: append across catalog") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.db.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      sql(s"CREATE TABLE $t2 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.insertInto(t1)
      spark.table(t1).write.insertInto(t2)
      checkAnswer(spark.table(t2), df)
    }
  }

  testQuietly("saveAsTable: table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table exists => append by name") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      // Default saveMode is append, therefore this doesn't throw a table already exists exception
      df.write.saveAsTable(t1)
      checkAnswer(spark.table(t1), df)

      // also appends are by name not by position
      df.select('data, 'id).write.saveAsTable(t1)
      checkAnswer(spark.table(t1), df.union(df))
    }
  }

  testQuietly("saveAsTable: table overwrite and table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("overwrite").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table overwrite and table exists => replace table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 'c', 'd'")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("overwrite").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: ignore mode and table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("ignore").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: ignore mode and table exists => do nothing") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 'c', 'd'")
      df.write.mode("ignore").saveAsTable(t1)
      checkAnswer(spark.table(t1), Seq(Row("c", "d")))
    }
  }

  SaveMode.values().foreach { mode =>
    test(s"save: new table creations with partitioning for table - mode: $mode") {
      val format = classOf[InMemoryV1Provider].getName
      val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
      df.write.mode(mode).option("name", "t1").format(format).partitionBy("a").save()

      checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
      assert(InMemoryV1Provider.tables("t1").schema === df.schema.asNullable)
      assert(InMemoryV1Provider.tables("t1").partitioning.sameElements(
        Array(IdentityTransform(FieldReference(Seq("a"))))))
    }

    test(s"save: new table creations with bucketing for table - mode: $mode") {
      val format = classOf[InMemoryV1Provider].getName
      val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")
      df.write.mode(mode).option("name", "t1").format(format).bucketBy(2, "a").save()

      checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
      assert(InMemoryV1Provider.tables("t1").schema === df.schema.asNullable)
      assert(InMemoryV1Provider.tables("t1").partitioning.sameElements(
        Array(BucketTransform(LiteralValue(2, IntegerType), Seq(FieldReference(Seq("a")))))))
    }
  }

  test("save: default mode is ErrorIfExists") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    // default is ErrorIfExists, and since a table already exists we throw an exception
    val e = intercept[AnalysisException] {
      df.write.option("name", "t1").format(format).partitionBy("a").save()
    }
    assert(e.getMessage.contains("already exists"))
  }

  test("save: Ignore mode") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    // no-op
    df.write.option("name", "t1").format(format).mode("ignore").partitionBy("a").save()

    checkAnswer(InMemoryV1Provider.getTableData(spark, "t1"), df)
  }

  test("save: tables can perform schema and partitioning checks if they already exist") {
    val format = classOf[InMemoryV1Provider].getName
    val df = Seq((1, "x"), (2, "y"), (3, "z")).toDF("a", "b")

    df.write.option("name", "t1").format(format).partitionBy("a").save()
    val e2 = intercept[IllegalArgumentException] {
      df.write.mode("append").option("name", "t1").format(format).partitionBy("b").save()
    }
    assert(e2.getMessage.contains("partitioning"))

    val e3 = intercept[IllegalArgumentException] {
      Seq((1, "x")).toDF("c", "d").write.mode("append").option("name", "t1").format(format)
        .save()
    }
    assert(e3.getMessage.contains("schema"))
  }
}

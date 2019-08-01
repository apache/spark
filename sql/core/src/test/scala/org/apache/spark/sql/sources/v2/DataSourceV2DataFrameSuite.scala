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

package org.apache.spark.sql.sources.v2

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import org.apache.spark.sql.{AnalysisException, QueryTest, SparkSession}
import org.apache.spark.sql.catalog.v2.{CatalogPlugin, Identifier}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetDataSourceV2, ParquetTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}

class DataSourceV2DataFrameSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {
  import testImplicits._

  private val v2Format = classOf[InMemoryTableProvider].getName
  private val dfData = Seq((1L, "a"), (2L, "b"), (3L, "c"))

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[TestInMemoryTableCatalog].getName)

    spark.createDataFrame(dfData).toDF("id", "data").createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.catalog("testcat").asInstanceOf[TestInMemoryTableCatalog].clearTables()
    spark.sql("DROP VIEW source")
    spark.sql("DROP VIEW source2")
  }

  private def sessionCatalogTest(testName: String)(f: SparkSession => Unit): Unit = {
    test("using session catalog: " + testName) {
      val catalogConf = SQLConf.V2_SESSION_CATALOG
      val newSession = spark.newSession()
      newSession.createDataFrame(dfData).toDF("id", "data").createOrReplaceTempView("source")
      newSession.sessionState.conf.setConf(catalogConf, classOf[TestV2SessionCatalog].getName)
      try f(newSession) finally {
        newSession.catalog("session").asInstanceOf[TestV2SessionCatalog].clearTables()
        newSession.sql("DROP VIEW source")
      }
    }
  }

  test("insertInto: append") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      spark.table("source").select("id", "data").write.insertInto(t1)
      checkAnswer(spark.table(t1), spark.table("source"))
    }
  }

  test("insertInto: append - across catalog") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.db.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 USING foo AS TABLE source")
      sql(s"CREATE TABLE $t2 (id bigint, data string) USING foo")
      spark.table(t1).write.insertInto(t2)
      checkAnswer(spark.table(t2), spark.table("source"))
    }
  }

  test("insertInto: append partitioned table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.table("source").write.insertInto(t1)
      checkAnswer(spark.table(t1), spark.table("source"))
    }
  }

  test("insertInto: overwrite non-partitioned table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS TABLE source")
      spark.table("source2").write.mode("overwrite").insertInto(t1)
      checkAnswer(spark.table(t1), spark.table("source2"))
    }
  }

  test("insertInto: overwrite - static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").write.insertInto(t1)
        spark.table("source").write.mode("overwrite").insertInto(t1)
        checkAnswer(spark.table(t1), spark.table("source"))
      }
    }
  }

  test("insertInto: overwrite - dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data").write.insertInto(t1)
        spark.table("source").write.mode("overwrite").insertInto(t1)
        checkAnswer(spark.table(t1),
          spark.table("source").union(sql("SELECT 4L, 'keep'")))
      }
    }
  }

  test("saveAsTable: with defined catalog and table doesn't exist") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      spark.table("source").select("id", "data").write.saveAsTable(t1)
      checkAnswer(spark.table(t1), spark.table("source"))
    }
  }

  test("saveAsTable: with defined catalog and table exists") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      spark.table("source").select("id", "data").write.saveAsTable(t1)
      checkAnswer(spark.table(t1), spark.table("source"))
    }
  }

  sessionCatalogTest("saveAsTable and v2 table - table doesn't exist") { session =>
    val t1 = "tbl"
    session.table("source").select("id", "data").write.format(v2Format).saveAsTable(t1)
    checkAnswer(session.table(t1), session.table("source"))
  }

  sessionCatalogTest("saveAsTable: with session catalog and v2 table - table exists") { session =>
    val t1 = "tbl"
    session.sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    intercept[TableAlreadyExistsException] {
      session.table("source").select("id", "data").write.format(v2Format).saveAsTable(t1)
    }
    session.table("source").select("id", "data")
      .write.format(v2Format).mode("append").saveAsTable(t1)
    checkAnswer(session.table(t1), session.table("source"))
  }
}

class InMemoryTableProvider extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException("D'oh!")
  }
}

/** A SessionCatalog that always loads an in memory Table, so we can test write code paths. */
class TestV2SessionCatalog extends V2SessionCatalog {

  protected val tables: util.Map[Identifier, InMemoryTable] =
    new ConcurrentHashMap[Identifier, InMemoryTable]()

  override def loadTable(ident: Identifier): Table = {
    if (tables.containsKey(ident)) {
      tables.get(ident)
    } else {
      super.loadTable(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val t = new InMemoryTable(ident.name(), schema, partitions, properties)
    tables.put(ident, t)
    t
  }

  def clearTables(): Unit = {
    tables.keySet().asScala.foreach(super.dropTable)
    tables.clear()
  }
}

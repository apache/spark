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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.catalog.v2.{CatalogPlugin, Identifier}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.utils.TestV2SessionCatalogBase
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceV2DataFrameSessionCatalogSuite
  extends SessionCatalogTest[InMemoryTable, InMemoryTableSessionCatalog] {

  test("saveAsTable: Append mode should not fail if the table already exists " +
    "and a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        val format = spark.sessionState.conf.defaultDataSourceName
        sql(s"CREATE TABLE same_name(id LONG) USING $format")
        spark.range(10).createTempView("same_name")
        spark.range(20).write.format(v2Format).mode(SaveMode.Append).saveAsTable("same_name")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        checkAnswer(spark.table("default.same_name"), spark.range(20).toDF())
      }
    }
  }

  test("saveAsTable with mode Overwrite should not fail if the table already exists " +
    "and a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        sql(s"CREATE TABLE same_name(id LONG) USING $v2Format")
        spark.range(10).createTempView("same_name")
        spark.range(20).write.format(v2Format).mode(SaveMode.Overwrite).saveAsTable("same_name")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        checkAnswer(spark.table("default.same_name"), spark.range(20).toDF())
      }
    }
  }
}

class InMemoryTableProvider extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException("D'oh!")
  }
}

class InMemoryTableSessionCatalog extends TestV2SessionCatalogBase[InMemoryTable] {
  override def newTable(
      name: String,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): InMemoryTable = {
    new InMemoryTable(name, schema, partitions, properties)
  }
}

private[v2] trait SessionCatalogTest[T <: Table, Catalog <: TestV2SessionCatalogBase[T]]
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfter {

  protected def catalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }

  protected val v2Format = classOf[InMemoryTableProvider].getName

  protected val catalogClassName: String = classOf[InMemoryTableSessionCatalog].getName

  before {
    spark.conf.set(SQLConf.V2_SESSION_CATALOG.key, catalogClassName)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    catalog("session").asInstanceOf[Catalog].clearTables()
    spark.conf.set(SQLConf.V2_SESSION_CATALOG.key, classOf[V2SessionCatalog].getName)
  }

  protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
    checkAnswer(sql(s"SELECT * FROM $tableName"), expected)
    checkAnswer(sql(s"SELECT * FROM default.$tableName"), expected)
    checkAnswer(sql(s"TABLE $tableName"), expected)
  }

  import testImplicits._

  test("saveAsTable: v2 table - table doesn't exist and default mode (ErrorIfExists)") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    df.write.format(v2Format).saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: v2 table - table doesn't exist and append mode") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    df.write.format(v2Format).mode("append").saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: Append mode should not fail if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.format(v2Format).mode(SaveMode.Append).saveAsTable("same_name")
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable: v2 table - table exists") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    spark.sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    intercept[TableAlreadyExistsException] {
      df.select("id", "data").write.format(v2Format).saveAsTable(t1)
    }
    df.write.format(v2Format).mode("append").saveAsTable(t1)
    verifyTable(t1, df)

    // Check that appends are by name
    df.select('data, 'id).write.format(v2Format).mode("append").saveAsTable(t1)
    verifyTable(t1, df.union(df))
  }

  test("saveAsTable: v2 table - table overwrite and table doesn't exist") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    df.write.format(v2Format).mode("overwrite").saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: v2 table - table overwrite and table exists") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    spark.sql(s"CREATE TABLE $t1 USING $v2Format AS SELECT 'c', 'd'")
    df.write.format(v2Format).mode("overwrite").saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: Overwrite mode should not drop the temp view if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.format(v2Format).mode(SaveMode.Overwrite).saveAsTable("same_name")
        assert(spark.sessionState.catalog.getTempView("same_name").isDefined)
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable: v2 table - ignore mode and table doesn't exist") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    df.write.format(v2Format).mode("ignore").saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: v2 table - ignore mode and table exists") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    spark.sql(s"CREATE TABLE $t1 USING $v2Format AS SELECT 'c', 'd'")
    df.write.format(v2Format).mode("ignore").saveAsTable(t1)
    verifyTable(t1, Seq(("c", "d")).toDF("id", "data"))
  }
}

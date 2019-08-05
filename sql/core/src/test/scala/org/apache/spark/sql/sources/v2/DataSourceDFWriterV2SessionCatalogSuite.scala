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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalog.v2.Identifier
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceDFWriterV2SessionCatalogSuite
  extends QueryTest
  with SharedSQLContext
  with BeforeAndAfter {
  import testImplicits._

  private val v2Format = classOf[InMemoryTableProvider].getName
  private val dfData = Seq((1L, "a"), (2L, "b"), (3L, "c"))

  before {
    spark.conf.set(SQLConf.V2_SESSION_CATALOG.key, classOf[TestV2SessionCatalog].getName)
    spark.createDataFrame(dfData).toDF("id", "data").createOrReplaceTempView("source")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.catalog("session").asInstanceOf[TestV2SessionCatalog].clearTables()
  }

  test("saveAsTable and v2 table - table doesn't exist") {
    val t1 = "tbl"
    spark.table("source").write.format(v2Format).saveAsTable(t1)
    checkAnswer(spark.table(t1), spark.table("source"))
  }

  test("saveAsTable: v2 table - table exists") {
    val t1 = "tbl"
    spark.sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    intercept[TableAlreadyExistsException] {
      spark.table("source").select("id", "data").write.format(v2Format).saveAsTable(t1)
    }
    spark.table("source").write.format(v2Format).mode("append").saveAsTable(t1)
    checkAnswer(spark.table(t1), spark.table("source"))

    // Check that appends are by name
    spark.table("source").select('data, 'id).write.format(v2Format).mode("append").saveAsTable(t1)
    checkAnswer(spark.table(t1), spark.table("source").union(spark.table("source")))
  }

  test("saveAsTable: v2 table - table overwrite and table doesn't exist") {
    val t1 = "tbl"
    spark.table("source").write.format(v2Format).mode("overwrite").saveAsTable(t1)
    checkAnswer(spark.table(t1), spark.table("source"))
  }

  test("saveAsTable: v2 table - table overwrite and table exists") {
    val t1 = "tbl"
    spark.sql(s"CREATE TABLE $t1 USING $v2Format AS SELECT 'c', 'd'")
    spark.table("source").write.format(v2Format).mode("overwrite").saveAsTable(t1)
    checkAnswer(spark.table(t1), spark.table("source"))
  }

  test("saveAsTable: v2 table - ignore mode and table doesn't exist") {
    val t1 = "tbl"
    spark.table("source").write.format(v2Format).mode("ignore").saveAsTable(t1)
    checkAnswer(spark.table(t1), spark.table("source"))
  }

  test("saveAsTable: v2 table - ignore mode and table exists") {
    val t1 = "tbl"
    spark.sql(s"CREATE TABLE $t1 USING $v2Format AS SELECT 'c', 'd'")
    spark.table("source").write.format(v2Format).mode("ignore").saveAsTable(t1)
    checkAnswer(spark.table(t1), Seq(Row("c", "d")))
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
      // Table was created through the built-in catalog
      val t = super.loadTable(ident)
      val table = new InMemoryTable(t.name(), t.schema(), t.partitioning(), t.properties())
      tables.put(ident, table)
      table
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
    assert(!tables.isEmpty, "Tables were empty, maybe didn't use the session catalog code path?")
    tables.keySet().asScala.foreach(super.dropTable)
    tables.clear()
  }
}

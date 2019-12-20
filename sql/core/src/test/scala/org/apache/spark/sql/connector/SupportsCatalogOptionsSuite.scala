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

import scala.language.implicitConversions

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsCatalogOptions}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SupportsCatalogOptionsSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  import testImplicits._

  private val catalogName = "testcat"
  private val format = classOf[CatalogSupportingInMemoryTableProvider].getName

  private def catalog(name: String): InMemoryTableSessionCatalog = {
    spark.sessionState.catalogManager.catalog(name).asInstanceOf[InMemoryTableSessionCatalog]
  }

  private implicit def stringToIdentifier(value: String): Identifier = {
    Identifier.of(Array.empty, value)
  }

  before {
    spark.conf.set(
      V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[InMemoryTableSessionCatalog].getName)
    spark.conf.set(
      s"spark.sql.catalog.$catalogName", classOf[InMemoryTableSessionCatalog].getName)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    catalog(SESSION_CATALOG_NAME).clearTables()
    catalog(catalogName).clearTables()
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    spark.conf.unset(s"spark.sql.catalog.$catalogName")
  }

  private def testCreateAndRead(
      saveMode: SaveMode,
      withCatalogOption: Option[String],
      partitionBy: Seq[String]): Unit = {
    val df = spark.range(10).withColumn("part", 'id % 5)
    val dfw = df.write.format(format).mode(saveMode).option("name", "t1")
    withCatalogOption.foreach(cName => dfw.option("catalog", cName))
    dfw.partitionBy(partitionBy: _*).save()

    val table = catalog(withCatalogOption.getOrElse(SESSION_CATALOG_NAME)).loadTable("t1")
    assert(table.name() === "t1", "Table identifier was wrong")
    assert(table.partitioning().length === partitionBy.length, "Partitioning did not match")
    assert(table.partitioning().map(_.references().head.fieldNames().head) === partitionBy,
      "Partitioning was incorrect")
    assert(table.schema() === df.schema.asNullable, "Schema did not match")

    val dfr = spark.read.format(format).option("name", "t1")
    withCatalogOption.foreach(cName => dfr.option("catalog", cName))
    checkAnswer(dfr.load(), df.toDF())
  }

  test(s"save works with ErrorIfExists - no table, no partitioning, session catalog") {
    testCreateAndRead(SaveMode.ErrorIfExists, None, Nil)
  }

  test(s"save works with ErrorIfExists - no table, with partitioning, session catalog") {
    testCreateAndRead(SaveMode.ErrorIfExists, None, Seq("part"))
  }

  test(s"save works with Ignore - no table, no partitioning, testcat catalog") {
    testCreateAndRead(SaveMode.ErrorIfExists, Some(catalogName), Nil)
  }

  test(s"save works with Ignore - no table, with partitioning, testcat catalog") {
    testCreateAndRead(SaveMode.ErrorIfExists, Some(catalogName), Seq("part"))
  }

  test("save fails with ErrorIfExists if table exists - session catalog") {
    sql("create table t1 (id bigint) using foo")
    val df = spark.range(10)
    intercept[TableAlreadyExistsException] {
      val dfw = df.write.format(format).option("name", "t1")
      dfw.save()
    }
  }

  test("save fails with ErrorIfExists if table exists - testcat catalog") {
    sql("create table t1 (id bigint) using foo")
    val df = spark.range(10)
    intercept[TableAlreadyExistsException] {
      val dfw = df.write.format(format).option("name", "t1").option("catalog", catalogName)
      dfw.save()
    }
  }

  test("Ignore mode if table exists - session catalog") {
    sql("create table t1 (id bigint) using foo")
    val df = spark.range(10).withColumn("part", 'id % 5)
    intercept[TableAlreadyExistsException] {
      val dfw = df.write.format(format).mode(SaveMode.Ignore).option("name", "t1")
      dfw.save()
    }

    val table = catalog(SESSION_CATALOG_NAME).loadTable("t1")
    assert(table.partitioning().isEmpty, "Partitioning should be empty")
    assert(table.schema() === new StructType().add("id", LongType), "Schema did not match")
  }

  test("Ignore mode if table exists - testcat catalog") {
    sql("create table t1 (id bigint) using foo")
    val df = spark.range(10).withColumn("part", 'id % 5)
    intercept[TableAlreadyExistsException] {
      val dfw = df.write.format(format).mode(SaveMode.Ignore).option("name", "t1")
      dfw.option("catalog", catalogName).save()
    }

    val table = catalog(catalogName).loadTable("t1")
    assert(table.partitioning().isEmpty, "Partitioning should be empty")
    assert(table.schema() === new StructType().add("id", LongType), "Schema did not match")
  }
}

class CatalogSupportingInMemoryTableProvider
  extends InMemoryTableProvider
  with SupportsCatalogOptions {

  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    val name = options.get("name")
    assert(name != null, "The name should be provided for this table")
    Identifier.of(Array.empty, name)
  }

  override def extractCatalog(options: CaseInsensitiveStringMap): String = {
    options.get("catalog")
  }
}

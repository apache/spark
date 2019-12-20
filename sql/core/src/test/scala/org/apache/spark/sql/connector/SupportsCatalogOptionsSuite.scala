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
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsCatalogOptions, TableCatalog}
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

  def testWithDifferentCatalogs(withCatalogOption: Option[String]): Unit = {
    Seq(SaveMode.ErrorIfExists, SaveMode.Ignore).foreach { saveMode =>
      test(s"save works with $saveMode - no table, no partitioning, session catalog, " +
           s"withCatalog: ${withCatalogOption.isDefined}") {
        val df = spark.range(10)
        val dfw = df.write.format(format).mode(saveMode).option("name", "t1")
        withCatalogOption.foreach(cName => dfw.option("catalog", cName))
        dfw.save()

        val table = catalog(withCatalogOption.getOrElse(SESSION_CATALOG_NAME)).loadTable("t1")
        assert(table.name() === "t1", "Table identifier was wrong")
        assert(table.partitioning().isEmpty, "Partitioning should be empty")
        assert(table.schema() === df.schema.asNullable, "Schema did not match")

        val dfr = spark.read.format(format).option("name", "t1")
        withCatalogOption.foreach(cName => dfr.option("catalog", cName))
        checkAnswer(dfr.load(), df.toDF())
      }

      test(s"save works with $saveMode - no table, with partitioning, session catalog, " +
          s"withCatalog: ${withCatalogOption.isDefined}") {
        val df = spark.range(10).withColumn("part", 'id % 5)
        val dfw = df.write.format(format).mode(saveMode).option("name", "t1").partitionBy("part")
        withCatalogOption.foreach(cName => dfw.option("catalog", cName))
        dfw.save()

        val table = catalog(withCatalogOption.getOrElse(SESSION_CATALOG_NAME)).loadTable("t1")
        assert(table.name() === "t1", "Table identifier was wrong")
        assert(table.partitioning().length === 1, "Partitioning should not be empty")
        assert(table.partitioning().head.references().head.fieldNames().head === "part",
          "Partitioning was incorrect")
        assert(table.schema() === df.schema.asNullable, "Schema did not match")

        val dfr = spark.read.format(format).option("name", "t1")
        withCatalogOption.foreach(cName => dfr.option("catalog", cName))
        checkAnswer(dfr.load(), df.toDF())
      }
    }

    test(s"save fails with ErrorIfExists if table exists, withCatalog: ${withCatalogOption.isDefined}") {
      sql("create table t1 (id bigint) using foo")
      val df = spark.range(10)
      intercept[TableAlreadyExistsException] {
        val dfw = df.write.format(format).option("name", "t1")
        withCatalogOption.foreach(cName => dfw.option("catalog", cName))
        dfw.save()
      }
    }

    test(s"Ignore mode if table exists, withCatalog: ${withCatalogOption.isDefined}") {
      sql("create table t1 (id bigint) using foo")
      val df = spark.range(10).withColumn("part", 'id % 5)
      intercept[TableAlreadyExistsException] {
        val dfw = df.write.format(format).mode(SaveMode.Ignore).option("name", "t1")
        withCatalogOption.foreach(cName => dfw.option("catalog", cName))
        dfw.save()
      }

      val table = catalog(SESSION_CATALOG_NAME).loadTable("t1")
      assert(table.partitioning().isEmpty, "Partitioning should be empty")
      assert(table.schema() === new StructType().add("id", LongType), "Schema did not match")
    }
  }

  testWithDifferentCatalogs(None)

  testWithDifferentCatalogs(Some(catalogName))
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

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

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table}
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.util.Utils

class DatasourceV2SQLBase
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = true)
  with AlterTableTests {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected val v2Source = classOf[FakeV2Provider].getName
  override protected val v2Format = v2Source
  override protected val catalogAndNamespace = "testcat.ns1.ns2."
  protected val defaultUser: String = Utils.getCurrentUserName()

  protected def catalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }

  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override def getTableMetadata(tableName: String): Table = {
    val nameParts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val v2Catalog = catalog(nameParts.head).asTableCatalog
    val namespace = nameParts.drop(1).init.toArray
    v2Catalog.loadTable(Identifier.of(namespace, nameParts.last))
  }

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testpart", classOf[InMemoryPartitionTableCatalog].getName)
    spark.conf.set(
      "spark.sql.catalog.testcat_atomic", classOf[StagingInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    spark.conf.set(
      V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[InMemoryTableSessionCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.sessionState.catalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }
}

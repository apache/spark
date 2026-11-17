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

package org.apache.spark.sql.pipelines.graph

import java.sql.DriverManager
import java.util.Properties

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.connector.catalog.{Column, Identifier, Table => V2Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.pipelines.utils.{BaseCoreExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/**
 * Materialization tests against an H2-backed [[JDBCTableCatalog]]. That catalog appends every
 * table property other than the reserved ones to the `CREATE TABLE` statement as a DDL option, so
 * the declared-keys record must not reach it (see
 * [[PipelinesTableProperties.declaredPropertyKeys]]).
 */
class MaterializeTablesInJdbcCatalogSuite extends BaseCoreExecutionTest with SharedSparkSession {
  import testImplicits._

  private val catalogName = "h2_cat"
  private val namespace = "ns"
  private val tempDir = Utils.createTempDir()
  private val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalogName", classOf[RecordingJDBCTableCatalog].getName)
    .set(s"spark.sql.catalog.$catalogName.url", url)
    .set(s"spark.sql.catalog.$catalogName.driver", "org.h2.Driver")

  override def beforeAll(): Unit = {
    super.beforeAll()
    Utils.classForName("org.h2.Driver")
    spark.sql(s"CREATE NAMESPACE $catalogName.$namespace")
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterAll()
    }
  }

  private def jdbcCatalog: RecordingJDBCTableCatalog =
    spark.sessionState.catalogManager.catalog(catalogName).asInstanceOf[RecordingJDBCTableCatalog]

  /** The comment H2 stores for a table in `namespace`. */
  private def h2TableComment(table: String): Option[String] = {
    val conn = DriverManager.getConnection(url, new Properties())
    try {
      val rs = conn.createStatement().executeQuery(
        "SELECT REMARKS FROM INFORMATION_SCHEMA.TABLES " +
        s"WHERE TABLE_SCHEMA = '$namespace' AND TABLE_NAME = '$table'")
      if (rs.next()) Option(rs.getString(1)) else None
    } finally {
      conn.close()
    }
  }

  test("SPARK-57670: a materialized view that declares a table property is created without the " +
    "declared-keys record") {
    // The JDBC catalog turns `comment` into the table comment and appends every other declared
    // property to the CREATE TABLE statement, where H2 accepts no such option. So the view declares
    // its comment as a table property, and the record would be the statement's only option.
    val declared = Map("comment" -> "view comment")
    // No format: the JDBC catalog rejects a `provider`, as it does for a definition with `USING`.
    materializeGraph(new TestGraphRegistrationContext(spark, defaultFormat = None) {
      registerMaterializedView(
        "mv",
        query = dfFlowFunc(Seq(1).toDF("id")),
        properties = declared,
        catalog = Some(catalogName),
        database = Some(namespace))
    }.resolveToDataflowGraph(), storageRoot = storageRoot)

    assert(jdbcCatalog.recordedCreateProperties == Seq(declared))
    assert(spark.table(s"$catalogName.$namespace.mv").schema.fieldNames === Seq("id"))
    assert(h2TableComment("mv").contains("view comment"))
  }
}

/** A [[JDBCTableCatalog]] that records the properties passed to each `createTable`. */
class RecordingJDBCTableCatalog extends JDBCTableCatalog {
  val recordedCreateProperties: mutable.ArrayBuffer[Map[String, String]] =
    mutable.ArrayBuffer.empty

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): V2Table = {
    recordedCreateProperties += properties.asScala.toMap
    super.createTable(ident, columns, partitions, properties)
  }
}

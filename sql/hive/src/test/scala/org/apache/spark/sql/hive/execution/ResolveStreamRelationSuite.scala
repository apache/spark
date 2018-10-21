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

package org.apache.spark.sql.hive.execution

import java.net.URI
import java.nio.file._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Test whether UnresolvedStreamRelation can be transformed tp StreamRelation or LogicalRelation
 */
class ResolveStreamRelationSuite extends AnalysisTest with TestHiveSingleton {

  protected val sqlConf = hiveContext.conf

  private val tempPath: String = {
    val temp = Paths.get("/tmp/somewhere")
    if (!Files.exists(temp)) {
      Files.createDirectory(temp)
    }
    temp.toString
  }

  protected val csvOptions: Map[String, String] = Map(
    "isStreaming" -> "true",
    "source" -> "csv",
    "path" -> tempPath,
    "sep" -> "\t"
  )

  protected val parquetOptions: Map[String, String] = Map(
    "isStreaming" -> "true",
    "source" -> "parquet",
    "path" -> tempPath
  )

  // the basic table schema for tests
  protected val tableSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType)
  ))

  /**
   * Generate Catalog table
   * @param tableName
   * @param sourceName
   * @param options
   * @return
   */
  protected def getCatalogTable(tableName: String,
      sourceName: String,
      options: Map[String, String]): CatalogTable = {
    val storage = DataSource.buildStorageFormatFromOptions(options)
    CatalogTable(
      identifier = TableIdentifier(tableName, Some("default")),
      tableType = CatalogTableType.MANAGED,
      provider = Some(sourceName),
      storage = storage,
      schema = tableSchema,
      partitionColumnNames = Nil)
  }

  protected val prepareTables: Seq[CatalogTable] = Seq(
    getCatalogTable("csvTable", "csv", csvOptions),
    getCatalogTable("parquetTable", "parquet", parquetOptions)
  )

  // overwrite this method in order to change sqlConf in tests
  override protected def getAnalyzer(caseSensitive: Boolean) = {
    val conf = sqlConf.copy(SQLConf.CASE_SENSITIVE -> caseSensitive)
    val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, conf)
    catalog.createDatabase(
      CatalogDatabase("default", "", new URI("loc"), Map.empty),
      ignoreIfExists = false)
    prepareTables.foreach{ table =>
      catalog.createTable(table, ignoreIfExists = true)
    }
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new ResolveStreamRelation(catalog, conf, spark) ::
          new ValidSQLStreaming(spark, conf) ::
          new FindDataSourceTable(spark) :: Nil
    }
  }

  protected def getStreamRelation(
      sourceName: String,
      options: Map[String, String]): StreamingRelation =
    StreamingRelation(
      DataSource(
        sparkSession = spark,
        className = sourceName,
        options = options,
        userSpecifiedSchema = Some(tableSchema),
        partitionColumns = Seq()
      )
    )

  test("resolve stream relations with manage table") {
    assertAnalysisError(UnresolvedStreamRelation(TableIdentifier("tAbLe")), Seq())
  }

  test("resolve stream relations with csv stream tabl") {
    checkAnalysis(
      UnresolvedStreamRelation(TableIdentifier("csvTable")),
      getStreamRelation("csv", csvOptions)
    )
  }

  test("resolve stream relations with parquet stream table") {
    checkAnalysis(
      UnresolvedStreamRelation(TableIdentifier("parquetTable")),
      getStreamRelation("parquet", parquetOptions)
    )
  }

  test("resolve stream relations with watermark") {
    val column: String = "timestamp"
    val delay: String = "2 seconds"
    sqlConf.setConf(SQLConf.SQLSTREAM_WATERMARK_ENABLE, true)
    sqlConf.setConfString("spark.sqlstreaming.watermark.default.csvTable.column", column)
    sqlConf.setConfString("spark.sqlstreaming.watermark.default.csvTable.delay", delay)
    assertAnalysisSuccess(
      UnresolvedStreamRelation(TableIdentifier("csvTable"))
    )
  }
}

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

package org.apache.spark.sql.hive

import java.net.URI

import scala.collection.mutable

import org.mockito.Mockito.when
import org.scalatest.PrivateMethodTester
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.internal.config.Tests
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType

class SubstituteMvOSSuite extends QueryTest with SQLTestUtils
  with PrivateMethodTester with MockitoSugar {

  var spark: SparkSession = _
  var catalog: SessionCatalog = _
  private val mockCatalog: MvCatalog = mock[HiveMvCatalog]

  // Private method accessors
  private val mvConfName = SQLConf.ENABLE_MV_OS_OPTIMIZATION.key
  private val convertParquetConfName = HiveUtils.CONVERT_METASTORE_PARQUET.key

  private var dataSourceTable1: CatalogTable = _
  private var dataSourceTable2: CatalogTable = _
  private var dataSourceTable3: CatalogTable = _
  private var simpleMvTable: CatalogTable = _
  private var mvTable1: CatalogTable = _
  private var mvTable2: CatalogTable = _
  private var mvTable3: CatalogTable = _
  private val tablesCreated: mutable.Seq[CatalogTable] = mutable.Seq.empty

  def getPlan(catalogTable: CatalogTable): Option[LogicalPlan] = {
    val viewText = catalogTable.viewOriginalText
    // val plan = sparkSession.sessionState.sqlParser.parsePlan(viewText.get)
    val optimizedPlan = sql(viewText.get).queryExecution.optimizedPlan
    Some(optimizedPlan)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = {
      // sparkConf.set(HiveUtils.HIVE_METASTORE_VERSION, "3.1.1")
      val builder = SparkSession.builder()
        .config(UI_ENABLED.key, "false")
        .config(Tests.IS_TESTING.key, "true" )
        .master("local[1]")
        .appName("Materialized views")
        .enableHiveSupport()
      builder.getOrCreate()
    }

    catalog = spark.sessionState.catalog

    catalog.createDatabase(newDb("db"), ignoreIfExists = true)
    var ident = TableIdentifier("tbl", Some("db"))
    catalog.dropTable(ident, ignoreIfNotExists = true, purge = true)
    val serde = HiveSerDe.sourceToSerDe("parquet")
    dataSourceTable1 = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db")),
      tableType = CatalogTableType.MANAGED,
      storage = getCatalogStorageFormat(serde),
      schema = new StructType()
        .add("id", "int").add("col1", "string"),
      provider = Some(DDLUtils.HIVE_PROVIDER))
    catalog.createTable(dataSourceTable1, ignoreIfExists = true)
    val ds1 = catalog.getTableMetadata(TableIdentifier("tbl", Some("db")))
    tablesCreated :+ ds1

    catalog.dropTable(TableIdentifier("tbl1", Some("db")), ignoreIfNotExists = true, purge = true)
    dataSourceTable2 = CatalogTable(
      identifier = TableIdentifier("tbl1", Some("db")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", "int").add("b", "string").add("c", "int"),
      provider = Some("parquet"))
    catalog.createTable(dataSourceTable2, ignoreIfExists = false)
    tablesCreated :+ dataSourceTable2

    catalog.dropTable(TableIdentifier("tbl2", Some("db")), ignoreIfNotExists = true, purge = true)
    dataSourceTable3 = CatalogTable(
      identifier = TableIdentifier("tbl2", Some("db")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("a", "int").add("b", "string").add("c", "int"),
      provider = Some("parquet"))
    catalog.createTable(dataSourceTable3, ignoreIfExists = false)
    tablesCreated :+ dataSourceTable3

    ident = TableIdentifier("mv", Some("db"))
    catalog.dropTable(ident, ignoreIfNotExists = true, purge = true)
    simpleMvTable = CatalogTable(
      identifier = TableIdentifier("mv", Some("db")),
      tableType = CatalogTableType.MV,
      storage = getCatalogStorageFormat(serde),
      schema = new StructType()
        .add("id", "int").add("col1", "string"),
      viewOriginalText = Some("SELECT * FROM db.tbl ORDER BY id"),
      viewText = Some("SELECT * FROM db.tbl ORDER BY id"),
      provider = Some(DDLUtils.HIVE_PROVIDER))
    catalog.createTable(simpleMvTable, ignoreIfExists = false)
    simpleMvTable = catalog.getTableMetadata(TableIdentifier("mv", Some("db")))
    tablesCreated :+ simpleMvTable

    val mvSchema = new StructType()
      .add("a", "int").add("b", "string").add("c", "int")

    catalog.dropTable(TableIdentifier("mv1", Some("db")), ignoreIfNotExists = true, purge = true)
    mvTable1 = createMVTable(" * ", "a > 10",
      "db.tbl1", "mv1", "db", mvSchema, catalog)
    tablesCreated :+ mvTable1

    catalog.dropTable(TableIdentifier("mv2", Some("db")), ignoreIfNotExists = true, purge = true)
    mvTable2 = createMVTable("a, b", "a > 5",
      "db.tbl1", "mv2", "db", mvSchema, catalog)
    tablesCreated :+ mvTable2

     /**
      *  reduced DNF: (a > 10) or (a > 5 and b < 20) or (b < 10 and a > 10) or (b < 10)
      */
    catalog.dropTable(TableIdentifier("mv3", Some("db")), ignoreIfNotExists = true, purge = true)
    mvTable3 = createMVTable("a, b, c", "(a > 5 or c < 10) and (a > 10 or c < 20)",
      "db.tbl2", "mv3", "db", mvSchema, catalog)
    tablesCreated :+ mvTable3

    when(mockCatalog.getMaterializedViewForTable("db", "tbl"))
      .thenReturn(CatalogCreationData("db", "tbl", Seq(("db", "mv"))))

    /* when(mockCatalog.getMaterializedViewsOfTable(Seq(("db", "mv"))))
      .thenReturn(Seq(simpleMvTable)) */

    val maybePlan = getPlan(simpleMvTable)
    when(mockCatalog.getMaterializedViewPlan(simpleMvTable))
      .thenReturn(maybePlan)

    when(mockCatalog.getMaterializedViewForTable("db", "tbl1"))
      .thenReturn(CatalogCreationData("db", "tbl1", Seq(("db", "mv1"), ("db", "mv2"))))
    /* when(mockCatalog.getMaterializedViewsOfTable(Seq(("db", "mv1"))))
      .thenReturn(Seq(mvTable1))
    when(mockCatalog.getMaterializedViewsOfTable(Seq(("db", "mv2"))))
      .thenReturn(Seq(mvTable2)) */
    when(mockCatalog.getMaterializedViewPlan(mvTable1))
      .thenReturn(getPlan(mvTable1))
    when(mockCatalog.getMaterializedViewPlan(mvTable2))
      .thenReturn(getPlan(mvTable2))

    when(mockCatalog.getMaterializedViewForTable("db", "tbl2"))
      .thenReturn(CatalogCreationData("db", "tbl2", Seq(("db", "mv3"))))
    /* when(mockCatalog.getMaterializedViewsOfTable(Seq(("db", "mv3"))))
      .thenReturn(Seq(mvTable3)) */
    when(mockCatalog.getMaterializedViewPlan(mvTable3))
      .thenReturn(getPlan(mvTable3))
  }

  def newDb(name: String): CatalogDatabase = {
    CatalogDatabase(name, "desc", new URI("loc"), Map())
  }


  test("Optimizer should substitute materialized view with non equality predicates") {
    withSQLConf((mvConfName, "true")) {
      // spark.sharedState.mvCatalog.init(spark) // we should move this inside session creation

      val df1 = sql("select * from db.tbl1 where a > 20")
      val df2 = sql("select * from db.mv1 where a > 20")
      val optimized1 = Optimize.execute(df1.queryExecution.analyzed)
      val optimized2 = Optimize.execute(df2.queryExecution.analyzed)

      comparePlans(optimized1, optimized2)
    }
  }

  test ("Check MV substitute with complex expression") {
    val queryPredicates1 = "(a > 10 and c < 5) or (a > 20)"
    val queryPredicates2 = "(a > 0 or c < 5)"
    var mvDf1: DataFrame = null
    var nonMvDf1: DataFrame = null
    withSQLConf((mvConfName, "false")) {
      // spark.sharedState.mvCatalog.init(spark)
      mvDf1 = sql(s"select a, c from ${mvTable3.identifier} where $queryPredicates1 ")
      nonMvDf1 = sql(s"select a, c from db.tbl1 where $queryPredicates2 ")
    }

    withSQLConf((mvConfName, "true")) {
      // spark.sharedState.mvCatalog.init(spark)
      val queryDf1 = sql(s"select a, c from db.tbl2 where $queryPredicates1 ")
      val queryDf2 = sql(s"select a, c from db.tbl1 where $queryPredicates2 ")

      comparePlans(Optimize.execute(queryDf1.queryExecution.analyzed),
        Optimize.execute(mvDf1.queryExecution.analyzed))
      // no substitution
      comparePlans(Optimize.execute(queryDf2.queryExecution.analyzed),
        Optimize.execute(nonMvDf1.queryExecution.analyzed))
    }
  }

  test("Optimizer should substitute materialized view mv2 and not mv1") {
    var optimized1 : LogicalPlan = null
    withSQLConf((mvConfName, "false")) {
      spark.sharedState.mvCatalog.init(spark)
      val df1 = sql(s"select a from ${mvTable2.identifier} where a > 7")
      optimized1 = Optimize.execute(df1.queryExecution.analyzed)
    }

    withSQLConf((mvConfName, "true")) {
      val df2 = sql("select a from db.tbl1 where a > 7")
      val optimized2 = Optimize.execute(df2.queryExecution.analyzed)
      comparePlans(optimized1, optimized2)
    }
  }

  test("Optimizer should not substitute materialized view due to column mismatch") {
    // Obtain Plan without MV enabled and it should be same as plan obtained when MV enabled.
    var optimized1 : LogicalPlan = null
    withSQLConf((mvConfName, "false")) {
      val df1 = sql("select * from db.tbl1 where a > 7")
      optimized1 = Optimize.execute(df1.queryExecution.analyzed)
    }

    var optimized2 : LogicalPlan = null
    withSQLConf((mvConfName, "true")) {
      spark.sharedState.mvCatalog.init(spark)

      val df2 = sql("select * from db.tbl1 where a > 7")
      optimized2 = Optimize.execute(df2.queryExecution.analyzed)
    }
    comparePlans(optimized1, optimized2)
  }

  test("Optimizer should substitute materialized view even with aggregations without group by") {
    var optimized1 : LogicalPlan = null
    // var optimized2 : LogicalPlan = null
    // var optimized3 : LogicalPlan = null
    withSQLConf((mvConfName, "false")) {
      val df1 = sql("select max(a) from db.mv2 where a > 7")
      optimized1 = Optimize.execute(df1.queryExecution.analyzed)

      // val df3 = sql("select count(case (c < 0) then 0 else 1) from mvdb.mv1 where a > 20")
      // optimized3 = df3.queryExecution.optimizedPlan
    }

    withSQLConf((mvConfName, "true")) {
      spark.sharedState.mvCatalog.init(spark)

      val df1 = sql("select max(a) from db.tbl1 where a > 7")
      val optimized4 = Optimize.execute(df1.queryExecution.analyzed)
      comparePlans(optimized1, optimized4)

      // val df3 = sql("select count(case (c < 0) then 0 else 1) from db1.tbl1 where a > 20")
      // val optimized6 = df3.queryExecution.optimizedPlan
      // comparePlans(optimized3, optimized6)
    }
  }

  test("Optimizer should substitute materialized view even with aggregations with group by") {
    var optimized1 : LogicalPlan = null
    var optimized2 : LogicalPlan = null
    // var optimized3 : LogicalPlan = null
    // we can merge `false` and `true` blocks? because MVs wont be substituted
    withSQLConf((mvConfName, "false")) {
      val df1 = sql("select max(a) from db.mv2 where a > 7 group by b")
      optimized1 = Optimize.execute(df1.queryExecution.analyzed)

      val df2 = sql("select count(*) from db.mv1 where a > 20 group by b")
      optimized2 = Optimize.execute(df2.queryExecution.analyzed)

      // val df3 = sql("select count(case (c < 0) then 0 else 1) from mvdb.mv1 where a > 20" +
      //  " group by b")
      // optimized3 = df3.queryExecution.optimizedPlan
    }

    withSQLConf((mvConfName, "true")) {
      spark.sharedState.mvCatalog.init(spark)

      val df1 = sql("select max(a) from db.tbl1 where a > 7 group by b")
      val optimized4 = Optimize.execute(df1.queryExecution.analyzed)
      comparePlans(optimized1, optimized4)

      val df2 = sql("select count(*) from db.tbl1 where a > 20 group by b")
      val optimized5 = Optimize.execute(df2.queryExecution.analyzed)
      comparePlans(optimized2, optimized5)

      // val df3 = sql("select count(case (c < 0) then 0 else 1) from db1.tbl1 where a > 20" +
      //  " group by b")
      // val optimized6 = df3.queryExecution.optimizedPlan
      // comparePlans(optimized3, optimized6)
    }
  }

  override protected def afterAll(): Unit = {
    try {
      tablesCreated.foreach(table =>
        catalog.dropTable(table.identifier, ignoreIfNotExists = false, purge = false))
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan): Unit = {
    val normalizedPlan1 = invalidateStatsCache(plan1)
    val normalizedPlan2 = invalidateStatsCache(plan2)
    super.comparePlans(normalizedPlan1, normalizedPlan2)
  }

  test("Optimizer should substitute materialized view") {
    spark.sharedState.mvCatalog.init(spark) // we should move this inside session creation
    var optimized1: LogicalPlan = null
    var optimized2: LogicalPlan = null
    withSQLConf((mvConfName, "true"),
      (convertParquetConfName, "true")) {
      val df1 = spark.sql("select * from db.tbl where id = 20")
      optimized1 = Optimize.execute(df1.queryExecution.analyzed)
      val df2 = spark.sql("select * from db.mv where id = 20")
      optimized2 = Optimize.execute(df2.queryExecution.analyzed)
      comparePlans(optimized1, optimized2)
    }
  }

  private def invalidateStatsCache(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case rel: HiveTableRelation =>
        val table = rel.tableMeta
        if (table.stats.isDefined) {
          rel.copy(tableMeta = table.copy(stats = None))
        } else {
          rel
        }
    }
  }

  private def createMVTable(projection: String, exp: String,
      originalTable: String, mvTable: String, mvDb: String, schema: StructType,
      catalog: SessionCatalog): CatalogTable = {
    val serde = HiveSerDe.sourceToSerDe("parquet")
    val table = CatalogTable(
      identifier = TableIdentifier(mvTable, Some(mvDb)),
      tableType = CatalogTableType.MV,
      storage = getCatalogStorageFormat(serde),
      schema,
      viewText = Some(s"select $projection from $originalTable where $exp"),
      viewOriginalText = Some(s"select $projection from $originalTable where $exp"),
      provider = Some(DDLUtils.HIVE_PROVIDER))
    catalog.createTable(table, ignoreIfExists = false)
    catalog.getTableMetadata(TableIdentifier(mvTable, Some(mvDb)))
  }

  private def getCatalogStorageFormat(serde: Option[HiveSerDe]): CatalogStorageFormat = {
    CatalogStorageFormat.empty.copy(
      inputFormat = serde.get.inputFormat,
      outputFormat = serde.get.outputFormat,
      serde = serde.flatMap(_.serde)
        .orElse(Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
  }

  private object Optimize extends RuleExecutor[LogicalPlan] {
    override protected def batches : Seq[Optimize.Batch] = {
      Seq(Batch("Substitute MV",
        Once,
        EliminateSubqueryAliases,
        SubstituteMaterializedOSView(mockCatalog.asInstanceOf[HiveMvCatalog])))
    }
  }


}


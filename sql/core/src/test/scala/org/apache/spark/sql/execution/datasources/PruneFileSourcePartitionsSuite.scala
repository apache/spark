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

package org.apache.spark.sql.execution.datasources

import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, Expression, In, Literal, Or}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class PruneFileSourcePartitionsSuite extends PrunePartitionSuiteBase with SharedSparkSession {
  type listPartitionLambda = Seq[Expression] => Unit
  type filterPartitionLambda = (Seq[CatalogTablePartition], Seq[Expression]) => Unit

  override def beforeEach(): Unit = {
    super.beforeEach()
    System.gc()
  }

  override def format: String = "parquet"

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PruneFileSourcePartitions", Once, PruneFileSourcePartitions) :: Nil
  }

  test("PruneFileSourcePartitions should not change the output of LogicalRelation") {
    withTable("test") {
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("test")
      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
      val catalogFileIndex = new CatalogFileIndex(spark, tableMeta, 0)

      val dataSchema = StructType(tableMeta.schema.filterNot { f =>
        tableMeta.partitionColumnNames.contains(f.name)
      })
      val relation = HadoopFsRelation(
        location = catalogFileIndex,
        partitionSchema = tableMeta.partitionSchema,
        dataSchema = dataSchema,
        bucketSpec = None,
        fileFormat = new ParquetFileFormat(),
        options = Map.empty)(sparkSession = spark)

      val logicalRelation = LogicalRelation(relation, tableMeta)
      val query = Project(Seq($"id", $"p"),
        Filter($"p" === 1, logicalRelation)).analyze

      val optimized = Optimize.execute(query)
      assert(optimized.missingInput.isEmpty)
    }
  }

  test("SPARK-20986 Reset table's statistics after PruneFileSourcePartitions rule") {
    withTable("tbl") {
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
      sql(s"ANALYZE TABLE tbl COMPUTE STATISTICS")
      val tableStats = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tbl")).stats
      assert(tableStats.isDefined && tableStats.get.sizeInBytes > 0, "tableStats is lost")

      val df = sql("SELECT * FROM tbl WHERE p = 1")
      val sizes1 = df.queryExecution.analyzed.collect {
        case relation: LogicalRelation => relation.catalogTable.get.stats.get.sizeInBytes
      }
      assert(sizes1.size === 1, s"Size wrong for:\n ${df.queryExecution}")
      assert(sizes1(0) == tableStats.get.sizeInBytes)

      val relations = df.queryExecution.optimizedPlan.collect {
        case relation: LogicalRelation => relation
      }
      assert(relations.size === 1, s"Size wrong for:\n ${df.queryExecution}")
      val size2 = relations(0).stats.sizeInBytes
      assert(size2 == relations(0).catalogTable.get.stats.get.sizeInBytes)
      assert(size2 < tableStats.get.sizeInBytes)
    }
  }

  test("SPARK-26576 Broadcast hint not applied to partitioned table") {
    withTable("tbl") {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
        val df = spark.table("tbl")
        val qe = df.join(broadcast(df), "p").queryExecution
        qe.sparkPlan.collect { case j: BroadcastHashJoinExec => j } should have size 1
      }
    }
  }

  test("SPARK-35985 push filters for empty read schema") {
    // Force datasource v2 for parquet
    withSQLConf((SQLConf.USE_V1_SOURCE_LIST.key, "")) {
      withTempPath { dir =>
        spark.range(10).coalesce(1).selectExpr("id", "id % 3 as p")
            .write.partitionBy("p").parquet(dir.getCanonicalPath)
        withTempView("tmp") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tmp")
          assertPrunedPartitions("SELECT COUNT(*) FROM tmp WHERE p = 0", 1, "(tmp.p = 0)")
          assertPrunedPartitions("SELECT input_file_name() FROM tmp WHERE p = 0", 1, "(tmp.p = 0)")
        }
      }
    }
  }

  test("SPARK-38357: data + partition filters with OR") {
    // Force datasource v2 for parquet
    withSQLConf((SQLConf.USE_V1_SOURCE_LIST.key, "")) {
      withTempPath { dir =>
        spark.range(10).coalesce(1).selectExpr("id", "id % 3 as p")
          .write.partitionBy("p").parquet(dir.getCanonicalPath)
        withTempView("tmp") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tmp")
          assertPrunedPartitions("SELECT * FROM tmp WHERE (p = 0 AND id > 0) OR (p = 1 AND id = 2)",
            2,
            "((tmp.p = 0) || (tmp.p = 1))")
          assertPrunedPartitions("SELECT * FROM tmp WHERE p = 0 AND id > 0",
            1,
            "(tmp.p = 0)")
          assertPrunedPartitions("SELECT * FROM tmp WHERE p = 0",
            1,
            "(tmp.p = 0)")
        }
      }
    }
  }

  test("SPARK-40565: don't push down non-deterministic filters for V2 file sources") {
    // Force datasource v2 for parquet
    withSQLConf((SQLConf.USE_V1_SOURCE_LIST.key, "")) {
      withTempPath { dir =>
        spark.range(10).coalesce(1).selectExpr("id", "id % 3 as p")
          .write.partitionBy("p").parquet(dir.getCanonicalPath)
        withTempView("tmp") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tmp")
          assertPrunedPartitions("SELECT * FROM tmp WHERE rand() > 0.5", 3, "")
          assertPrunedPartitions("SELECT * FROM tmp WHERE p > rand()", 3, "")
          assertPrunedPartitions("SELECT * FROM tmp WHERE p = 0 AND rand() > 0.5",
            1,
            "(tmp.p = 0)")
        }
      }
    }
  }

  test("no duplicates in union filter and no redundant filter evaluation") {
    withTable("test") {
      var listPartionCBInvoked: Boolean = false
      var filterPartitionsCBInvoked: Boolean = false
      executeTestLogic(
        logicalRelation => {
          val q1 = Project(Seq($"id".as("id1"), $"p".as("p1")),
            Filter($"p" === 1, logicalRelation)).analyze
          val q2 = Project(Seq($"id".as("id2"), $"p".as("p2")),
            Filter($"p" === 1, logicalRelation)).analyze
          val query = q1.join(q2, JoinType("cross"))
          Optimize.execute(query)
        },
        Option((exprs: Seq[Expression]) => {
          listPartionCBInvoked = true
          assert(exprs.lengthCompare(1) == 0)
          exprs.head match {
            case BinaryComparison(_, _) =>
            case _ => fail("distinct filter expression not found")
          }
        }),
        Option((_: Seq[CatalogTablePartition], exprs: Seq[Expression]) => {
          filterPartitionsCBInvoked = true
          assert(exprs.isEmpty)
        })
      )
      assert(listPartionCBInvoked)
      assert(filterPartitionsCBInvoked)
    }
  }

  test("union should contain all distinct filters") {
    withTable("test") {
      var listPartionCBInvoked: Boolean = false
      var filterPartitionsCBInvoked: Boolean = false
      executeTestLogic(
        logicalRelation => {
          val q1 = Project(Seq($"id".as("id1"), $"p".as("p1")),
            Filter($"p" === 2, logicalRelation)).analyze
          val q2 = Project(Seq($"id".as("id2"), $"p".as("p2")),
            Filter($"p" > 1 && $"p" < 7, logicalRelation)).analyze
          val query = q1.join(q2, JoinType("cross"))
          Optimize.execute(query)
        },
        Option((exprs: Seq[Expression]) => {
          listPartionCBInvoked = true
          assert(exprs.lengthCompare(1) == 0)
          exprs.head match {
            case Or(_: BinaryComparison, And(_: BinaryComparison, _: BinaryComparison)) =>
            case Or(And(_: BinaryComparison, _: BinaryComparison), _: BinaryComparison) =>
            case _ => fail("distinct filter expression not found")
          }
        }),
        Option((_: Seq[CatalogTablePartition], exprs: Seq[Expression]) => {
          filterPartitionsCBInvoked = true
          assert(exprs.lengthCompare(1) == 0)
          exprs.head match {
            case _: BinaryComparison =>
            case And(_: BinaryComparison, _: BinaryComparison) =>
            case _ => fail("Binary Comparison expression not found")
          }
        })
      )
      assert(listPartionCBInvoked)
      assert(filterPartitionsCBInvoked)
    }
  }

  test("No pruning filter in even a single table should result in no pruning at all") {
    withTable("test") {
      var listPartionCBInvoked: Boolean = false
      var filterPartitionsCBInvoked: Boolean = false
      executeTestLogic(
        logicalRelation => {
          val q1 = Project(Seq($"id".as("id1"), $"p".as("p1")),
            Filter($"p" === 2, logicalRelation)).analyze
          val q2 = Project(Seq($"id".as("id2"), $"p".as("p2")),
            Filter(In($"p", Seq(Literal(1), Literal(4))), logicalRelation)).analyze
          val q3 = Project(Seq($"id".as("id2"), $"p".as("p2")), logicalRelation).analyze
          val query = q1.join(q2, JoinType("cross")).join(q3, JoinType("cross"))
          Optimize.execute(query)
        },
        Option((exprs: Seq[Expression]) => {
          listPartionCBInvoked = true
          assert(exprs.isEmpty)
        }),
        Option((_: Seq[CatalogTablePartition], exprs: Seq[Expression]) => {
          filterPartitionsCBInvoked = true
          exprs.headOption match {
            case None =>
            case Some(_: BinaryComparison) =>
            case Some(_: In) =>
            case _ => fail("Binary Comparison expression not found")
          }
        })
      )
      assert(listPartionCBInvoked)
      assert(filterPartitionsCBInvoked)
    }
  }

  test("order of the repeating tables should not impact the unioned filter") {
    withTable("test") {
      var listPartionCBInvoked: Boolean = false
      var filterPartitionsCBInvoked: Boolean = false
      val listCbLmbda: Option[listPartitionLambda] = Option((exprs: Seq[Expression]) => {
        listPartionCBInvoked = true
        assert(exprs.isEmpty)
      })
      val filterCbLmbda: Option[filterPartitionLambda] =
        Option((_: Seq[CatalogTablePartition], exprs: Seq[Expression]) => {
          filterPartitionsCBInvoked = true
          exprs.headOption match {
            case None =>
            case Some(_: BinaryComparison) =>
            case Some(_: In) =>
            case _ => fail("Binary Comparison expression not found")
          }
        })
      executeTestLogic(
        logicalRelation => {
          val q1 = Project(Seq($"id".as("id1"), $"p".as("p1")),
            Filter($"p" === 2, logicalRelation)).analyze
          val q2 = Project(Seq($"id".as("id2"), $"p".as("p2")),
            Filter(In($"p", Seq(Literal(1), Literal(4))), logicalRelation)).analyze
          val q3 = Project(Seq($"id".as("id2"), $"p".as("p2")), logicalRelation).analyze
          val query = q1.join(q2, JoinType("cross")).join(q3, JoinType("cross"))
          Optimize.execute(query)
        },
        listCbLmbda,
        filterCbLmbda
      )
      assert(listPartionCBInvoked)
      assert(filterPartitionsCBInvoked)

      listPartionCBInvoked = false
      filterPartitionsCBInvoked = false
      executeTestLogic(
        logicalRelation => {
          val q1 = Project(Seq($"id".as("id1"), $"p".as("p1")),
            Filter($"p" === 2, logicalRelation)).analyze
          val q2 = Project(Seq($"id".as("id2"), $"p".as("p2")),
            Filter(In($"p", Seq(Literal(1), Literal(4))), logicalRelation)).analyze
          val q3 = Project(Seq($"id".as("id2"), $"p".as("p2")), logicalRelation).analyze
          val query = q2.join(q3, JoinType("cross")).join(q1, JoinType("cross"))
          Optimize.execute(query)
        },
        listCbLmbda,
        filterCbLmbda
      )
      assert(listPartionCBInvoked)
      assert(filterPartitionsCBInvoked)

      listPartionCBInvoked = false
      filterPartitionsCBInvoked = false
      executeTestLogic(
        logicalRelation => {
          val q1 = Project(Seq($"id".as("id1"), $"p".as("p1")),
            Filter($"p" === 2, logicalRelation)).analyze
          val q2 = Project(Seq($"id".as("id2"), $"p".as("p2")),
            Filter(In($"p", Seq(Literal(1), Literal(4))), logicalRelation)).analyze
          val q3 = Project(Seq($"id".as("id2"), $"p".as("p2")), logicalRelation).analyze
          val query = q3.join(q1, JoinType("cross")).join(q2, JoinType("cross"))
          Optimize.execute(query)
        },
        listCbLmbda,
        filterCbLmbda
      )
      assert(listPartionCBInvoked)
      assert(filterPartitionsCBInvoked)
    }
  }

  private def executeTestLogic(
      body: LogicalRelation => Unit,
      listPartitionCb: Option[listPartitionLambda],
      filterPartitionCb: Option[filterPartitionLambda]): Unit = {
    spark.range(10).selectExpr("id", "id % 3 as p").write.mode(SaveMode.Overwrite).
      partitionBy("p").saveAsTable("test")
    val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
    val catalogFileIndex = new CallbackCatalogFileIndex(spark, tableMeta, 0, listPartitionCb,
      filterPartitionCb)
    val dataSchema = StructType(tableMeta.schema.filterNot { f =>
      tableMeta.partitionColumnNames.contains(f.name)
    })
    val relation = HadoopFsRelation(
      location = catalogFileIndex,
      partitionSchema = tableMeta.partitionSchema,
      dataSchema = dataSchema,
      bucketSpec = None,
      fileFormat = new ParquetFileFormat(),
      options = Map.empty)(sparkSession = spark)

    val logicalRelation = LogicalRelation(relation, tableMeta)
    // execute test body
    body(logicalRelation)
  }

  class CallbackCatalogFileIndex(
      sparkSession: SparkSession,
      table: CatalogTable,
      sizeInBytes: Long,
      val onListPartitionsInvoc: Option[listPartitionLambda] = None,
      val onFilterPartitionsInvoc: Option[filterPartitionLambda] = None)
    extends CatalogFileIndex(sparkSession, table, sizeInBytes) {

    override def listPartitions(filters: Seq[Expression]): (Seq[CatalogTablePartition], Long) = {
      onListPartitionsInvoc.foreach(_.apply(filters))
      super.listPartitions(filters)
    }

    override def filterPartitions(
        inputPartitions: Seq[CatalogTablePartition],
        baseTimeNs: Long,
        filters: Seq[Expression]): InMemoryFileIndex = {
      onFilterPartitionsInvoc.foreach(_.apply(inputPartitions, filters))
      super.filterPartitions(inputPartitions, baseTimeNs, filters)
    }
  }

  protected def collectPartitionFiltersFn(): PartialFunction[SparkPlan, Seq[Expression]] = {
    case scan: FileSourceScanExec => scan.partitionFilters
  }

  override def getScanExecPartitionSize(plan: SparkPlan): Long = {
    plan.collectFirst {
      case p: FileSourceScanExec => p.selectedPartitions.partitionCount
      case BatchScanExec(_, scan: FileScan, _, _, _, _) =>
        scan.fileIndex.listFiles(scan.partitionFilters, scan.dataFilters).length
    }.get
  }
}

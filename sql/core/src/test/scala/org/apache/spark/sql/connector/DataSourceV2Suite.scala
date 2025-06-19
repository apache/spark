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

import java.io.File
import java.util
import java.util.OptionalLong

import test.org.apache.spark.sql.connector._

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{PartitionInternalRow, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, Literal, NamedReference, NullOrdering, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.Scan.ColumnarSupportMode
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ArrayImplicits._

class DataSourceV2Suite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def getBatch(query: DataFrame): AdvancedBatch = {
    query.queryExecution.executedPlan.collect {
      case d: BatchScanExec =>
        d.batch.asInstanceOf[AdvancedBatch]
    }.head
  }

  private def getBatchWithV2Filter(query: DataFrame): AdvancedBatchWithV2Filter = {
    query.queryExecution.executedPlan.collect {
      case d: BatchScanExec =>
        d.batch.asInstanceOf[AdvancedBatchWithV2Filter]
    }.head
  }

  private def getJavaBatch(query: DataFrame): JavaAdvancedDataSourceV2.AdvancedBatch = {
    query.queryExecution.executedPlan.collect {
      case d: BatchScanExec =>
        d.batch.asInstanceOf[JavaAdvancedDataSourceV2.AdvancedBatch]
    }.head
  }

  private def getJavaBatchWithV2Filter(
      query: DataFrame): JavaAdvancedDataSourceV2WithV2Filter.AdvancedBatchWithV2Filter = {
    query.queryExecution.executedPlan.collect {
      case d: BatchScanExec =>
        d.batch.asInstanceOf[JavaAdvancedDataSourceV2WithV2Filter.AdvancedBatchWithV2Filter]
    }.head
  }

  test("invalid data source") {
    intercept[IllegalArgumentException] {
      spark.read.format(classOf[InvalidDataSource].getName).load()
    }
  }

  test("simplest implementation") {
    Seq(classOf[SimpleDataSourceV2], classOf[JavaSimpleDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select($"j"), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter($"i" > 5), (6 until 10).map(i => Row(i, -i)))
      }
    }
  }

  test("advanced implementation") {
    Seq(classOf[AdvancedDataSourceV2], classOf[JavaAdvancedDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))

        val q1 = df.select($"j")
        checkAnswer(q1, (0 until 10).map(i => Row(-i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val batch = getBatch(q1)
          assert(batch.filters.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        } else {
          val batch = getJavaBatch(q1)
          assert(batch.filters.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        }

        val q2 = df.filter($"i" > 3)
        checkAnswer(q2, (4 until 10).map(i => Row(i, -i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val batch = getBatch(q2)
          assert(batch.filters.flatMap(_.references).toSet == Set("i"))
          assert(batch.requiredSchema.fieldNames === Seq("i", "j"))
        } else {
          val batch = getJavaBatch(q2)
          assert(batch.filters.flatMap(_.references).toSet == Set("i"))
          assert(batch.requiredSchema.fieldNames === Seq("i", "j"))
        }

        val q3 = df.select($"i").filter($"i" > 6)
        checkAnswer(q3, (7 until 10).map(i => Row(i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val batch = getBatch(q3)
          assert(batch.filters.flatMap(_.references).toSet == Set("i"))
          assert(batch.requiredSchema.fieldNames === Seq("i"))
        } else {
          val batch = getJavaBatch(q3)
          assert(batch.filters.flatMap(_.references).toSet == Set("i"))
          assert(batch.requiredSchema.fieldNames === Seq("i"))
        }

        val q4 = df.select($"j").filter($"j" < -10)
        checkAnswer(q4, Nil)
        if (cls == classOf[AdvancedDataSourceV2]) {
          val batch = getBatch(q4)
          // $"j" < 10 is not supported by the testing data source.
          assert(batch.filters.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        } else {
          val batch = getJavaBatch(q4)
          // $"j" < 10 is not supported by the testing data source.
          assert(batch.filters.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        }
      }
    }
  }

  test("advanced implementation with V2 Filter") {
    Seq(classOf[AdvancedDataSourceV2WithV2Filter], classOf[JavaAdvancedDataSourceV2WithV2Filter])
      .foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))

        val q1 = df.select($"j")
        checkAnswer(q1, (0 until 10).map(i => Row(-i)))
        if (cls == classOf[AdvancedDataSourceV2WithV2Filter]) {
          val batch = getBatchWithV2Filter(q1)
          assert(batch.predicates.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        } else {
          val batch = getJavaBatchWithV2Filter(q1)
          assert(batch.predicates.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        }

        val q2 = df.filter($"i" > 3)
        checkAnswer(q2, (4 until 10).map(i => Row(i, -i)))
        if (cls == classOf[AdvancedDataSourceV2WithV2Filter]) {
          val batch = getBatchWithV2Filter(q2)
          assert(batch.predicates.flatMap(_.references.map(_.describe)).toSet == Set("i"))
          assert(batch.requiredSchema.fieldNames === Seq("i", "j"))
        } else {
          val batch = getJavaBatchWithV2Filter(q2)
          assert(batch.predicates.flatMap(_.references.map(_.describe)).toSet == Set("i"))
          assert(batch.requiredSchema.fieldNames === Seq("i", "j"))
        }

        val q3 = df.select($"i").filter($"i" > 6)
        checkAnswer(q3, (7 until 10).map(i => Row(i)))
        if (cls == classOf[AdvancedDataSourceV2WithV2Filter]) {
          val batch = getBatchWithV2Filter(q3)
          assert(batch.predicates.flatMap(_.references.map(_.describe)).toSet == Set("i"))
          assert(batch.requiredSchema.fieldNames === Seq("i"))
        } else {
          val batch = getJavaBatchWithV2Filter(q3)
          assert(batch.predicates.flatMap(_.references.map(_.describe)).toSet == Set("i"))
          assert(batch.requiredSchema.fieldNames === Seq("i"))
        }

        val q4 = df.select($"j").filter($"j" < -10)
        checkAnswer(q4, Nil)
        if (cls == classOf[AdvancedDataSourceV2WithV2Filter]) {
          val batch = getBatchWithV2Filter(q4)
          // $"j" < 10 is not supported by the testing data source.
          assert(batch.predicates.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        } else {
          val batch = getJavaBatchWithV2Filter(q4)
          // $"j" < 10 is not supported by the testing data source.
          assert(batch.predicates.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        }
      }
    }
  }

  test("columnar batch scan implementation") {
    Seq(classOf[ColumnarDataSourceV2], classOf[JavaColumnarDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 90).map(i => Row(i, -i)))
        checkAnswer(df.select($"j"), (0 until 90).map(i => Row(-i)))
        checkAnswer(df.filter($"i" > 50), (51 until 90).map(i => Row(i, -i)))
      }
    }
  }

  test("schema required data source") {
    Seq(classOf[SchemaRequiredDataSource], classOf[JavaSchemaRequiredDataSource]).foreach { cls =>
      withClue(cls.getName) {
        val e = intercept[IllegalArgumentException](spark.read.format(cls.getName).load())
        assert(e.getMessage.contains("requires a user-supplied schema"))

        val schema = new StructType().add("i", "int").add("j", "int")
        val df = spark.read.format(cls.getName).schema(schema).load()

        assert(df.schema == schema)
        checkAnswer(df, Seq(Row(0, 0), Row(1, -1)))
      }
    }
  }

  test("SPARK-33369: Skip schema inference in DataframeWriter.save() if table provider " +
    "supports external metadata") {
    withTempDir { dir =>
      val cls = classOf[SupportsExternalMetadataWritableDataSource].getName
      spark.range(10).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
        .write.format(cls).option("path", dir.getCanonicalPath).mode("append").save()
      val schema = new StructType().add("i", "long").add("j", "long")
        checkAnswer(
          spark.read.format(cls).option("path", dir.getCanonicalPath).schema(schema).load(),
          spark.range(10).select($"id", -$"id"))
    }
  }

  test("partitioning reporting") {
    import org.apache.spark.sql.functions.{count, sum}
    withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "true") {
      Seq(classOf[PartitionAwareDataSource], classOf[JavaPartitionAwareDataSource]).foreach { cls =>
        withClue(cls.getName) {
          val df = spark.read.format(cls.getName).load()
          checkAnswer(df, Seq(Row(1, 4), Row(1, 4), Row(3, 6), Row(2, 6), Row(4, 2), Row(4, 2)))

          val groupByColI = df.groupBy($"i").agg(sum($"j"))
          checkAnswer(groupByColI, Seq(Row(1, 8), Row(2, 6), Row(3, 6), Row(4, 4)))
          assert(collectFirst(groupByColI.queryExecution.executedPlan) {
            case e: ShuffleExchangeExec => e
          }.isEmpty)

          val groupByColIJ = df.groupBy($"i", $"j").agg(count("*"))
          checkAnswer(groupByColIJ, Seq(Row(1, 4, 2), Row(2, 6, 1), Row(3, 6, 1), Row(4, 2, 2)))
          assert(collectFirst(groupByColIJ.queryExecution.executedPlan) {
            case e: ShuffleExchangeExec => e
          }.isEmpty)

          val groupByColJ = df.groupBy($"j").agg(sum($"i"))
          checkAnswer(groupByColJ, Seq(Row(2, 8), Row(4, 2), Row(6, 5)))
          assert(collectFirst(groupByColJ.queryExecution.executedPlan) {
            case e: ShuffleExchangeExec => e
          }.isDefined)

          val groupByIPlusJ = df.groupBy($"i" + $"j").agg(count("*"))
          checkAnswer(groupByIPlusJ, Seq(Row(5, 2), Row(6, 2), Row(8, 1), Row(9, 1)))
          assert(collectFirst(groupByIPlusJ.queryExecution.executedPlan) {
            case e: ShuffleExchangeExec => e
          }.isDefined)
        }
      }
    }
  }

  test("ordering and partitioning reporting") {
    withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "true") {
      Seq(
        classOf[OrderAndPartitionAwareDataSource],
        classOf[JavaOrderAndPartitionAwareDataSource]
      ).foreach { cls =>
        withClue(cls.getName) {
          // we test report ordering (together with report partitioning) with these transformations:
          // - groupBy("i").flatMapGroups:
          //   hash-partitions by "i" and sorts each partition by "i"
          //   requires partitioning and sort by "i"
          // - aggregation function over window partitioned by "i" and ordered by "j":
          //   hash-partitions by "i" and sorts each partition by "j"
          //   requires partitioning by "i" and sort by "i" and "j"
          Seq(
            // with no partitioning and no order, we expect shuffling AND sorting
            (None, None, (true, true), (true, true)),
            // partitioned by i and no order, we expect NO shuffling BUT sorting
            (Some("i"), None, (false, true), (false, true)),
            // partitioned by i and in-partition sorted by i,
            // we expect NO shuffling AND sorting for groupBy but sorting for window function
            (Some("i"), Some("i"), (false, false), (false, true)),
            // partitioned by i and in-partition sorted by j, we expect NO shuffling BUT sorting
            (Some("i"), Some("j"), (false, true), (false, true)),
            // partitioned by i and in-partition sorted by i,j, we expect NO shuffling NOR sorting
            (Some("i"), Some("i,j"), (false, false), (false, false)),
            // partitioned by j and in-partition sorted by i, we expect shuffling AND sorting
            (Some("j"), Some("i"), (true, true), (true, true)),
            // partitioned by j and in-partition sorted by i,j, we expect shuffling and sorting
            (Some("j"), Some("i,j"), (true, true), (true, true))
          ).foreach { testParams =>
            val (partitionKeys, orderKeys, groupByExpects, windowFuncExpects) = testParams

            withClue(f"${partitionKeys.orNull} ${orderKeys.orNull}") {
              val df = spark.read
                .option("partitionKeys", partitionKeys.orNull)
                .option("orderKeys", orderKeys.orNull)
                .format(cls.getName)
                .load()
              checkAnswer(df, Seq(Row(1, 4), Row(1, 5), Row(3, 5), Row(2, 6), Row(4, 1), Row(4, 2)))

              // groupBy(i).flatMapGroups
              {
                val groupBy = df.groupBy($"i").as[Int, (Int, Int)]
                  .flatMapGroups { (i: Int, it: Iterator[(Int, Int)]) =>
                    Iterator.single((i, it.length)) }
                checkAnswer(
                  groupBy.toDF(),
                  Seq(Row(1, 2), Row(2, 1), Row(3, 1), Row(4, 2))
                )

                val (shuffleExpected, sortExpected) = groupByExpects
                assert(collectFirst(groupBy.queryExecution.executedPlan) {
                  case e: ShuffleExchangeExec => e
                }.isDefined === shuffleExpected)
                assert(collectFirst(groupBy.queryExecution.executedPlan) {
                  case e: SortExec => e
                }.isDefined === sortExpected)
              }

              // aggregation function over window partitioned by i and ordered by j
              {
                val windowPartByColIOrderByColJ = df.withColumn("no",
                  row_number() over Window.partitionBy(Symbol("i")).orderBy(Symbol("j"))
                )
                checkAnswer(windowPartByColIOrderByColJ, Seq(
                  Row(1, 4, 1), Row(1, 5, 2), Row(2, 6, 1), Row(3, 5, 1), Row(4, 1, 1), Row(4, 2, 2)
                ))

                val (shuffleExpected, sortExpected) = windowFuncExpects
                assert(collectFirst(windowPartByColIOrderByColJ.queryExecution.executedPlan) {
                  case e: ShuffleExchangeExec => e
                }.isDefined === shuffleExpected)
                assert(collectFirst(windowPartByColIOrderByColJ.queryExecution.executedPlan) {
                  case e: SortExec => e
                }.isDefined === sortExpected)
              }
            }
          }
        }
      }
    }
  }

  test ("statistics report data source") {
    Seq(classOf[ReportStatisticsDataSource], classOf[JavaReportStatisticsDataSource]).foreach {
      cls =>
        withClue(cls.getName) {
          val df = spark.read.format(cls.getName).load()
          val logical = df.queryExecution.optimizedPlan.collect {
            case d: DataSourceV2ScanRelation => d
          }.head

          val statics = logical.computeStats()
          assert(statics.rowCount.isDefined && statics.rowCount.get === 10,
            "Row count statics should be reported by data source")
          assert(statics.sizeInBytes === 80,
            "Size in bytes statics should be reported by data source")
        }
    }
  }

  test("SPARK-23574: no shuffle exchange with single partition") {
    val df = spark.read.format(classOf[SimpleSinglePartitionSource].getName).load().agg(count("*"))
    assert(df.queryExecution.executedPlan.collect { case e: Exchange => e }.isEmpty)
  }

  test("SPARK-44505: should not call planInputPartitions() on explain") {
    val df = spark.read.format(classOf[ScanDefinedColumnarSupport].getName)
      .option("columnar", "PARTITION_DEFINED").load()
    // Default mode will throw an exception on explain.
    var ex = intercept[IllegalArgumentException](df.explain())
    assert(ex.getMessage == "planInputPartitions must not be called")

    Seq("SUPPORTED", "UNSUPPORTED").foreach { o =>
      val dfScan = spark.read.format(classOf[ScanDefinedColumnarSupport].getName)
        .option("columnar", o).load()
      dfScan.explain()
      //  Will fail during regular execution.
      ex = intercept[IllegalArgumentException](dfScan.count())
      assert(ex.getMessage == "planInputPartitions must not be called")
    }
  }

  test("simple writable data source") {
    Seq(classOf[SimpleWritableDataSource], classOf[JavaSimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        spark.range(10).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
          .write.format(cls.getName)
          .option("path", path).mode("append").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select($"id", -$"id"))

        // default save mode is ErrorIfExists
        intercept[AnalysisException] {
          spark.range(10).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
            .write.format(cls.getName)
            .option("path", path).save()
        }
        spark.range(10).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
          .write.mode("append").format(cls.getName)
          .option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).union(spark.range(10)).select($"id", -Symbol("id")))

        spark.range(5).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
          .write.format(cls.getName)
          .option("path", path).mode("overwrite").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select($"id", -$"id"))

        checkError(
          exception = intercept[AnalysisException] {
            spark.range(5).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
              .write.format(cls.getName)
              .option("path", path).mode("ignore").save()
          },
          condition = "UNSUPPORTED_DATA_SOURCE_SAVE_MODE",
          parameters = Map(
            "source" -> cls.getName,
            "createMode" -> "\"Ignore\""
          )
        )

        checkError(
          exception = intercept[AnalysisException] {
            spark.range(5).select($"id" as Symbol("i"), -$"id" as Symbol("j"))
              .write.format(cls.getName)
              .option("path", path).mode("error").save()
          },
          condition = "UNSUPPORTED_DATA_SOURCE_SAVE_MODE",
          parameters = Map(
            "source" -> cls.getName,
            "createMode" -> "\"ErrorIfExists\""
          )
        )
      }
    }
  }

  test("simple counter in writer with onDataWriterCommit") {
    Seq(classOf[SimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        val numPartition = 6
        spark.range(0, 10, 1, numPartition)
          .select($"id" as Symbol("i"), -$"id" as Symbol("j"))
          .write.format(cls.getName)
          .mode("append").option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select($"id", -$"id"))

        assert(SimpleCounter.getCounter == numPartition,
          "method onDataWriterCommit should be called as many as the number of partitions")
      }
    }
  }

  test("SPARK-23293: data source v2 self join") {
    val df = spark.read.format(classOf[SimpleDataSourceV2].getName).load()
    val df2 = df.select(($"i" + 1).as("k"), $"j")
    checkAnswer(df.join(df2, "j"), (0 until 10).map(i => Row(-i, i, i + 1)))
  }

  test("SPARK-23301: column pruning with arbitrary expressions") {
    val df = spark.read.format(classOf[AdvancedDataSourceV2].getName).load()

    val q1 = df.select($"i" + 1)
    checkAnswer(q1, (1 until 11).map(i => Row(i)))
    val batch1 = getBatch(q1)
    assert(batch1.requiredSchema.fieldNames === Seq("i"))

    val q2 = df.select(lit(1))
    checkAnswer(q2, (0 until 10).map(i => Row(1)))
    val batch2 = getBatch(q2)
    assert(batch2.requiredSchema.isEmpty)

    // 'j === 1 can't be pushed down, but we should still be able do column pruning
    val q3 = df.filter($"j" === -1).select($"j" * 2)
    checkAnswer(q3, Row(-2))
    val batch3 = getBatch(q3)
    assert(batch3.filters.isEmpty)
    assert(batch3.requiredSchema.fieldNames === Seq("j"))

    // column pruning should work with other operators.
    val q4 = df.sort($"i").limit(1).select($"i" + 1)
    checkAnswer(q4, Row(1))
    val batch4 = getBatch(q4)
    assert(batch4.requiredSchema.fieldNames === Seq("i"))
  }

  test("SPARK-23315: get output from canonicalized data source v2 related plans") {
    def checkCanonicalizedOutput(
        df: DataFrame, logicalNumOutput: Int, physicalNumOutput: Int): Unit = {
      val logical = df.queryExecution.analyzed.collect {
        case d: DataSourceV2Relation => d
      }.head
      assert(logical.canonicalized.output.length == logicalNumOutput)

      val physical = df.queryExecution.executedPlan.collect {
        case d: BatchScanExec => d
      }.head
      assert(physical.canonicalized.output.length == physicalNumOutput)
    }

    val df = spark.read.format(classOf[AdvancedDataSourceV2].getName).load()
    checkCanonicalizedOutput(df, 2, 2)
    checkCanonicalizedOutput(df.select($"i"), 2, 1)
  }

  test("SPARK-25425: extra options should override sessions options during reading") {
    val prefix = "spark.datasource.userDefinedDataSource."
    val optionName = "optionA"
    withSQLConf(prefix + optionName -> "true") {
      val df = spark
        .read
        .option(optionName, false)
        .format(classOf[DataSourceV2WithSessionConfig].getName).load()
      val options = df.queryExecution.analyzed.collectFirst {
        case d: DataSourceV2Relation => d.options
      }.get
      assert(options.get(optionName) === "false")
    }
  }

  test("SPARK-25425: extra options should override sessions options during writing") {
    withTempPath { path =>
      val sessionPath = path.getCanonicalPath
      withSQLConf("spark.datasource.simpleWritableDataSource.path" -> sessionPath) {
        withTempPath { file =>
          val optionPath = file.getCanonicalPath
          val format = classOf[SimpleWritableDataSource].getName

          val df = Seq((1L, 2L)).toDF("i", "j")
          df.write.format(format).mode("append").option("path", optionPath).save()
          assert(!new File(sessionPath).exists)
          checkAnswer(spark.read.format(format).option("path", optionPath).load(), df)
        }
      }
    }
  }

  test("SPARK-27411: DataSourceV2Strategy should not eliminate subquery") {
    withTempView("t1") {
      val t2 = spark.read.format(classOf[SimpleDataSourceV2].getName).load()
      Seq(2, 3).toDF("a").createTempView("t1")
      val df = t2.where("i < (select max(a) from t1)").select($"i")
      val subqueries = stripAQEPlan(df.queryExecution.executedPlan).collect {
        case p => p.subqueries
      }.flatten
      assert(subqueries.length == 1)
      checkAnswer(df, (0 until 3).map(i => Row(i)))
    }
  }

  test("SPARK-32609: DataSourceV2 with different pushedfilters should be different") {
    def getScanExec(query: DataFrame): BatchScanExec = {
      query.queryExecution.executedPlan.collect {
        case d: BatchScanExec => d
      }.head
    }

    Seq(classOf[AdvancedDataSourceV2], classOf[JavaAdvancedDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        val q1 = df.select($"i").filter($"i" > 6)
        val q2 = df.select($"i").filter($"i" > 5)
        val scan1 = getScanExec(q1)
        val scan2 = getScanExec(q2)
        assert(!scan1.equals(scan2))
      }
    }
  }

  test("SPARK-33267: push down with condition 'in (..., null)' should not throw NPE") {
    Seq(classOf[AdvancedDataSourceV2], classOf[JavaAdvancedDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        // before SPARK-33267 below query just threw NPE
        df.select($"i").where("i in (1, null)").collect()
      }
    }
  }

  test("SPARK-35803: Support datasorce V2 in CREATE VIEW USING") {
    Seq(classOf[SimpleDataSourceV2], classOf[JavaSimpleDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        sql(s"CREATE or REPLACE TEMPORARY VIEW s1 USING ${cls.getName}")
        checkAnswer(sql("select * from s1"), (0 until 10).map(i => Row(i, -i)))
        checkAnswer(sql("select j from s1"), (0 until 10).map(i => Row(-i)))
        checkAnswer(sql("select * from s1 where i > 5"),
          (6 until 10).map(i => Row(i, -i)))
      }
    }
  }

  test("SPARK-46043: create table in SQL using a DSv2 source") {
    Seq(classOf[SimpleDataSourceV2], classOf[JavaSimpleDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        // Create a table with empty schema.
        withTable("test") {
          sql(s"CREATE TABLE test USING ${cls.getName}")
          checkAnswer(
            sql(s"SELECT * FROM test WHERE i < 3"),
            Seq(Row(0, 0), Row(1, -1), Row(2, -2)))
        }
        // Create a table with non-empty schema is not allowed.
        checkError(
          exception = intercept[SparkUnsupportedOperationException] {
            sql(s"CREATE TABLE test(a INT, b INT) USING ${cls.getName}")
          },
          condition = "CANNOT_CREATE_DATA_SOURCE_TABLE.EXTERNAL_METADATA_UNSUPPORTED",
          parameters = Map("tableName" -> "`default`.`test`", "provider" -> cls.getName)
        )
      }
    }
  }

  test("SPARK-46043: create table in SQL with schema required data source") {
    val cls = classOf[SchemaRequiredDataSource]
    val e = intercept[IllegalArgumentException] {
      sql(s"CREATE TABLE test USING ${cls.getName}")
    }
    assert(e.getMessage.contains("requires a user-supplied schema"))
    withTable("test") {
      sql(s"CREATE TABLE test(i INT, j INT) USING ${cls.getName}")
      checkAnswer(sql(s"SELECT * FROM test"), Seq(Row(0, 0), Row(1, -1)))
    }
    withTable("test") {
      sql(s"CREATE TABLE test(i INT) USING ${cls.getName}")
      checkAnswer(sql(s"SELECT * FROM test"), Seq(Row(0), Row(1)))
    }
    withTable("test") {
      // Test the behavior when there is a mismatch between the schema defined in the
      // CREATE TABLE command and the actual schema produced by the data source. The
      // resulting behavior is not guaranteed and may vary based on the data source's
      // implementation.
      sql(s"CREATE TABLE test(i INT, j INT, k INT) USING ${cls.getName}")
      val e = intercept[Exception] {
        sql("SELECT * FROM test").collect()
      }
      assert(e.getMessage.contains(
        "java.lang.ArrayIndexOutOfBoundsException: Index 2 out of bounds for length 2"))
    }
  }

  test("SPARK-46043: create table in SQL with partitioning required data source") {
    val cls = classOf[PartitionsRequiredDataSource]
    val e = intercept[IllegalArgumentException](
      sql(s"CREATE TABLE test(a INT) USING ${cls.getName}"))
    assert(e.getMessage.contains("user-supplied partitioning"))
    withTable("test") {
      sql(s"CREATE TABLE test(i INT, j INT) USING ${cls.getName} PARTITIONED BY (i)")
      checkAnswer(sql(s"SELECT * FROM test"), Seq(Row(0, 0), Row(1, -1)))
    }
  }

  test("SPARK-46043: create table in SQL with path option") {
    val cls = classOf[WritableDataSourceSupportsExternalMetadata]
    withTempDir { dir =>
      val path = s"${dir.getCanonicalPath}/test"
      Seq((0, 1), (1, 2)).toDF("x", "y").write.format("csv").save(path)
      withTable("test") {
        sql(
          s"""
             |CREATE TABLE test USING ${cls.getName}
             |OPTIONS (PATH '$path')
             |""".stripMargin)
        checkAnswer(sql("SELECT * FROM test"), Seq(Row(0, 1), Row(1, 2)))
        sql(
          s"""
             |CREATE OR REPLACE TABLE test USING ${cls.getName}
             |OPTIONS (PATH '${dir.getCanonicalPath}/non-existing')
             |""".stripMargin)
        checkAnswer(sql("SELECT * FROM test"), Nil)
        sql(
          s"""
             |CREATE OR REPLACE TABLE test USING ${cls.getName}
             |LOCATION '$path'
             |""".stripMargin)
        checkAnswer(sql("SELECT * FROM test"), Seq(Row(0, 1), Row(1, 2)))
      }
    }
  }

  test("SPARK-46272: create table - schema mismatch") {
    withTable("test") {
      val cls = classOf[WritableDataSourceSupportsExternalMetadata]
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TABLE test (x INT, y INT) USING ${cls.getName}")
        },
        condition = "DATA_SOURCE_TABLE_SCHEMA_MISMATCH",
        parameters = Map(
          "dsSchema" -> "\"STRUCT<i: INT, j: INT>\"",
          "expectedSchema" -> "\"STRUCT<x: INT, y: INT>\""))
    }
  }

  test("SPARK-46272: create table as select") {
    val cls = classOf[WritableDataSourceSupportsExternalMetadata]
    withTable("test") {
      sql(
        s"""
           |CREATE TABLE test USING ${cls.getName}
           |AS VALUES (0, 1), (1, 2) t(i, j)
           |""".stripMargin)
      checkAnswer(sql("SELECT * FROM test"), Seq((0, 1), (1, 2)).toDF("i", "j"))
      sql(
        s"""
           |CREATE OR REPLACE TABLE test USING ${cls.getName}
           |AS VALUES (2, 3), (4, 5) t(i, j)
           |""".stripMargin)
      checkAnswer(sql("SELECT * FROM test"), Seq((2, 3), (4, 5)).toDF("i", "j"))
      sql(
        s"""
           |CREATE TABLE IF NOT EXISTS test USING ${cls.getName}
           |AS VALUES (3, 4), (4, 5)
           |""".stripMargin)
      checkAnswer(sql("SELECT * FROM test"), Seq((2, 3), (4, 5)).toDF("i", "j"))
    }
  }

  test("SPARK-46272: create table as select - schema name mismatch") {
    val cls = classOf[WritableDataSourceSupportsExternalMetadata]
    withTable("test") {
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TABLE test USING ${cls.getName} AS VALUES (0, 1), (1, 2)")
        },
        condition = "DATA_SOURCE_TABLE_SCHEMA_MISMATCH",
        parameters = Map(
          "dsSchema" -> "\"STRUCT<i: INT, j: INT>\"",
          "expectedSchema" -> "\"STRUCT<col1: INT, col2: INT>\""))
    }
  }

  test("SPARK-46272: create table as select - column type mismatch") {
    val cls = classOf[WritableDataSourceSupportsExternalMetadata]
    withTable("test") {
      checkError(
        exception = intercept[AnalysisException] {
          sql(
            s"""
               |CREATE TABLE test USING ${cls.getName}
               |AS VALUES ('a', 'b'), ('c', 'd') t(i, j)
               |""".stripMargin)
        },
        condition = "DATA_SOURCE_TABLE_SCHEMA_MISMATCH",
        parameters = Map(
          "dsSchema" -> "\"STRUCT<i: INT, j: INT>\"",
          "expectedSchema" -> "\"STRUCT<i: STRING, j: STRING>\""))
    }
  }

  test("SPARK-46272: create or replace table as select with path options") {
    val cls = classOf[CustomSchemaAndPartitioningDataSource]
    withTempDir { dir =>
      val path = s"${dir.getCanonicalPath}/test"
      Seq((0, 1), (1, 2)).toDF("x", "y").write.format("csv").save(path)
      withTable("test") {
        sql(
          s"""
             |CREATE TABLE test USING ${cls.getName}
             |OPTIONS (PATH '$path')
             |AS VALUES (0, 1)
             |""".stripMargin)
        checkAnswer(sql("SELECT * FROM test"), Seq(Row(0, 1), Row(0, 1), Row(1, 2)))
        // Check the data currently in the path location.
        checkAnswer(
          spark.read.format("csv").load(path),
          Seq(Row("0", "1"), Row("0", "1"), Row("1", "2")))
        // Replace the table with new data.
        sql(
          s"""
             |CREATE OR REPLACE TABLE test USING ${cls.getName}
             |OPTIONS (PATH '$path')
             |AS VALUES (2, 3)
             |""".stripMargin)
        checkAnswer(sql("SELECT * FROM test"), Seq(Row(2, 3)))
        // Replace the table without the path options.
        sql(
          s"""
             |CREATE OR REPLACE TABLE test USING ${cls.getName}
             |AS VALUES (3, 4)
             |""".stripMargin)
        checkAnswer(sql("SELECT * FROM test"), Seq(Row(3, 4)))
      }
    }
  }

  test("SPARK-46272: create table as select with incompatible data sources") {
    // CTAS with data sources that do not support external metadata.
    withTable("test") {
      val cls = classOf[SimpleDataSourceV2]
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          sql(s"CREATE TABLE test USING ${cls.getName} AS VALUES (0, 1)")
        },
        condition = "CANNOT_CREATE_DATA_SOURCE_TABLE.EXTERNAL_METADATA_UNSUPPORTED",
        parameters = Map(
          "tableName" -> "`default`.`test`",
          "provider" -> "org.apache.spark.sql.connector.SimpleDataSourceV2"))
    }
    // CTAS with data sources that do not support batch write.
    withTable("test") {
      val cls = classOf[SchemaRequiredDataSource]
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TABLE test USING ${cls.getName} AS SELECT * FROM VALUES (0, 1)")
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`test`",
          "operation" -> "append in batch mode"))
    }
  }

  test("SPARK-46273: insert into") {
    val cls = classOf[CustomSchemaAndPartitioningDataSource]
    withTable("test") {
      sql(
        s"""
           |CREATE TABLE test (x INT, y INT) USING ${cls.getName}
           |""".stripMargin)
      sql("INSERT INTO test VALUES (1, 2)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(1, 2)))
      // Insert by name
      sql("INSERT INTO test(y, x) VALUES (3, 2)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(1, 2), Row(2, 3)))
      // Can be casted automatically
      sql("INSERT INTO test(y, x) VALUES (4L, 3L)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(1, 2), Row(2, 3), Row(3, 4)))
      // Insert values by name
      sql("INSERT INTO test BY NAME VALUES (5, 4) t(y, x)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(1, 2), Row(2, 3), Row(3, 4), Row(4, 5)))
      // Missing columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO test VALUES (4)")
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`test`",
          "tableColumns" -> "`x`, `y`",
          "dataColumns" -> "`col1`"
        )
      )
      // Duplicate columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO test(x, x) VALUES (4, 5)")
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`x`"))
    }
  }

  test("SPARK-46273: insert overwrite") {
    val cls = classOf[CustomSchemaAndPartitioningDataSource]
    withTable("test") {
      sql(
        s"""
           |CREATE TABLE test USING ${cls.getName}
           |AS VALUES (0, 1), (1, 2) t(x, y)
           |""".stripMargin)
      sql(
        s"""
           |INSERT OVERWRITE test VALUES (2, 3), (3, 4)
           |""".stripMargin)
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(2, 3), Row(3, 4)))
      // Insert overwrite by name
      sql("INSERT OVERWRITE test(y, x) VALUES (3, 2)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(2, 3)))
      // Can be casted automatically
      sql("INSERT OVERWRITE test(y, x) VALUES (4L, 3L)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(3, 4)))
    }
  }

  test("SPARK-46273: insert into with partition") {
    val cls = classOf[CustomSchemaAndPartitioningDataSource]
    withTable("test") {
      sql(s"CREATE TABLE test(x INT, y INT) USING ${cls.getName} PARTITIONED BY (x, y)")
      sql("INSERT INTO test PARTITION(x = 1) VALUES (2)")
      sql("INSERT INTO test PARTITION(x = 2, y) VALUES (3)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(1, 2), Row(2, 3)))
      sql("INSERT INTO test PARTITION(y, x = 3) VALUES (4)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(1, 2), Row(2, 3), Row(3, 4)))
      sql("INSERT INTO test PARTITION(x, y) VALUES (4, 5)")
      checkAnswer(sql("SELECT * FROM test"), Seq(Row(1, 2), Row(2, 3), Row(3, 4), Row(4, 5)))
      checkError(
        exception = intercept[AnalysisException] {
          sql("INSERT INTO test PARTITION(z = 1) VALUES (2)")
        },
        condition = "NON_PARTITION_COLUMN",
        parameters = Map("columnName" -> "`z`"))
      checkError(
        exception = intercept[AnalysisException] {
          sql("INSERT INTO test PARTITION(x, y = 1) VALUES (2, 3)")
        },
        condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`test`",
          "tableColumns" -> "`x`, `y`",
          "dataColumns" -> "`col1`, `y`, `col2`")
      )
    }
  }

  test("SPARK-46273: insert overwrite with partition") {
    val cls = classOf[CustomSchemaAndPartitioningDataSource]
    withTable("test") {
      sql(s"CREATE TABLE test (x INT, y INT) USING ${cls.getName} PARTITIONED BY (x, y)")
      sql("INSERT INTO test PARTITION(x = 1) VALUES (2)")
      checkError(
        exception = intercept[AnalysisException] {
          sql("INSERT OVERWRITE test PARTITION(x = 1) VALUES (5)")
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`test`",
          "operation" -> "overwrite by filter in batch mode")
      )
    }
  }

  test("SPARK-47463: Pushed down v2 filter with if expression") {
    withTempView("t1") {
      spark.read.format(classOf[AdvancedDataSourceV2WithV2Filter].getName).load()
        .createTempView("t1")
      val df = sql("SELECT * FROM  t1 WHERE if(i = 1, i, 0) > 0")
      val result = df.collect()
      assert(result.length == 1)
    }
  }
}

case class RangeInputPartition(start: Int, end: Int) extends InputPartition

object SimpleReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val RangeInputPartition(start, end) = partition
    new PartitionReader[InternalRow] {
      private var current = start - 1

      override def next(): Boolean = {
        current += 1
        current < end
      }

      override def get(): InternalRow = InternalRow(current, -current)

      override def close(): Unit = {}
    }
  }
}

abstract class SimpleBatchTable extends Table with SupportsRead  {

  override def schema(): StructType = TestingV2Source.schema

  override def name(): String = this.getClass.toString

  override def capabilities(): java.util.Set[TableCapability] = java.util.EnumSet.of(BATCH_READ)
}

abstract class SimpleScanBuilder extends ScanBuilder
  with Batch with Scan {

  override def build(): Scan = this

  override def toBatch: Batch = this

  override def readSchema(): StructType = TestingV2Source.schema

  override def createReaderFactory(): PartitionReaderFactory = SimpleReaderFactory
}

trait TestingV2Source extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    TestingV2Source.schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties))
  }

  def getTable(options: CaseInsensitiveStringMap): Table
}

object TestingV2Source {
  val schema = new StructType().add("i", "int").add("j", "int")
}

class SimpleSinglePartitionSource extends TestingV2Source {

  class MyScanBuilder extends SimpleScanBuilder {
    override def planInputPartitions(): Array[InputPartition] = {
      Array(RangeInputPartition(0, 5))
    }
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder()
    }
  }
}

class ScanDefinedColumnarSupport extends TestingV2Source {

  class MyScanBuilder(st: ColumnarSupportMode) extends SimpleScanBuilder {
    override def planInputPartitions(): Array[InputPartition] = {
      throw new IllegalArgumentException("planInputPartitions must not be called")
    }

    override def columnarSupportMode() : ColumnarSupportMode = st

  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder(Scan.ColumnarSupportMode.valueOf(options.get("columnar")))
    }
  }

}


// This class is used by pyspark tests. If this class is modified/moved, make sure pyspark
// tests still pass.
class SimpleDataSourceV2 extends TestingV2Source {

  class MyScanBuilder extends SimpleScanBuilder {
    override def planInputPartitions(): Array[InputPartition] = {
      Array(RangeInputPartition(0, 5), RangeInputPartition(5, 10))
    }
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder()
    }
  }
}

class AdvancedDataSourceV2 extends TestingV2Source {

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new AdvancedScanBuilder()
    }
  }
}

class AdvancedScanBuilder extends ScanBuilder
  with Scan with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  var requiredSchema = TestingV2Source.schema
  var filters = Array.empty[Filter]

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def readSchema(): StructType = requiredSchema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition {
      case GreaterThan("i", _: Int) => true
      case _ => false
    }
    this.filters = supported
    unsupported
  }

  override def pushedFilters(): Array[Filter] = filters

  override def build(): Scan = this

  override def toBatch: Batch = new AdvancedBatch(filters, requiredSchema)
}

class AdvancedBatch(val filters: Array[Filter], val requiredSchema: StructType) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    val lowerBound = filters.collectFirst {
      case GreaterThan("i", v: Int) => v
    }

    val res = scala.collection.mutable.ArrayBuffer.empty[InputPartition]

    if (lowerBound.isEmpty) {
      res.append(RangeInputPartition(0, 5))
      res.append(RangeInputPartition(5, 10))
    } else if (lowerBound.get < 4) {
      res.append(RangeInputPartition(lowerBound.get + 1, 5))
      res.append(RangeInputPartition(5, 10))
    } else if (lowerBound.get < 9) {
      res.append(RangeInputPartition(lowerBound.get + 1, 10))
    }

    res.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new AdvancedReaderFactory(requiredSchema)
  }
}

class AdvancedDataSourceV2WithV2Filter extends TestingV2Source {

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new AdvancedScanBuilderWithV2Filter()
    }
  }
}

class AdvancedScanBuilderWithV2Filter extends ScanBuilder
  with Scan with SupportsPushDownV2Filters with SupportsPushDownRequiredColumns {

  var requiredSchema = TestingV2Source.schema
  var predicates = Array.empty[Predicate]

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def readSchema(): StructType = requiredSchema

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (supported, unsupported) = predicates.partition {
      case p: Predicate if p.name() == ">" => true
      case _ => false
    }
    this.predicates = supported
    unsupported
  }

  override def pushedPredicates(): Array[Predicate] = predicates

  override def build(): Scan = this

  override def toBatch: Batch = new AdvancedBatchWithV2Filter(predicates, requiredSchema)
}

class AdvancedBatchWithV2Filter(
    val predicates: Array[Predicate],
    val requiredSchema: StructType) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    val lowerBound = predicates.collectFirst {
      case p: Predicate if p.name().equals(">") =>
        val value = p.children()(1)
        assert(value.isInstanceOf[Literal[_]])
        value.asInstanceOf[Literal[_]]
    }

    val res = scala.collection.mutable.ArrayBuffer.empty[InputPartition]

    if (lowerBound.isEmpty) {
      res.append(RangeInputPartition(0, 5))
      res.append(RangeInputPartition(5, 10))
    } else if (lowerBound.get.value.asInstanceOf[Integer] < 4) {
      res.append(RangeInputPartition(lowerBound.get.value.asInstanceOf[Integer] + 1, 5))
      res.append(RangeInputPartition(5, 10))
    } else if (lowerBound.get.value.asInstanceOf[Integer] < 9) {
      res.append(RangeInputPartition(lowerBound.get.value.asInstanceOf[Integer] + 1, 10))
    }

    res.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new AdvancedReaderFactory(requiredSchema)
  }
}

class AdvancedReaderFactory(requiredSchema: StructType) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val RangeInputPartition(start, end) = partition
    new PartitionReader[InternalRow] {
      private var current = start - 1

      override def next(): Boolean = {
        current += 1
        current < end
      }

      override def get(): InternalRow = {
        val values = requiredSchema.map(_.name).map {
          case "i" => current
          case "j" => -current
        }
        InternalRow.fromSeq(values)
      }

      override def close(): Unit = {}
    }
  }
}


class SchemaRequiredDataSource extends TableProvider {

  class MyScanBuilder(schema: StructType) extends SimpleScanBuilder {
    override def planInputPartitions(): Array[InputPartition] = {
      Array(RangeInputPartition(0, 2))
    }

    override def readSchema(): StructType = schema
  }

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new IllegalArgumentException("requires a user-supplied schema")
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val userGivenSchema = schema
    new SimpleBatchTable {
      override def schema(): StructType = userGivenSchema

      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new MyScanBuilder(userGivenSchema)
      }
    }
  }
}

class PartitionsRequiredDataSource extends SchemaRequiredDataSource {
  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    throw new IllegalArgumentException("requires user-supplied partitioning")
  }
}

class ColumnarDataSourceV2 extends TestingV2Source {

  class MyScanBuilder extends SimpleScanBuilder {

    override def planInputPartitions(): Array[InputPartition] = {
      Array(RangeInputPartition(0, 50), RangeInputPartition(50, 90))
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      ColumnarReaderFactory
    }
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder()
    }
  }
}

object ColumnarReaderFactory extends PartitionReaderFactory {
  private final val BATCH_SIZE = 20

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw SparkUnsupportedOperationException()
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val RangeInputPartition(start, end) = partition
    new PartitionReader[ColumnarBatch] {
      private lazy val i = new OnHeapColumnVector(BATCH_SIZE, IntegerType)
      private lazy val j = new OnHeapColumnVector(BATCH_SIZE, IntegerType)
      private lazy val batch = new ColumnarBatch(Array(i, j))

      private var current = start

      override def next(): Boolean = {
        i.reset()
        j.reset()

        var count = 0
        while (current < end && count < BATCH_SIZE) {
          i.putInt(count, current)
          j.putInt(count, -current)
          current += 1
          count += 1
        }

        if (count == 0) {
          false
        } else {
          batch.setNumRows(count)
          true
        }
      }

      override def get(): ColumnarBatch = batch

      override def close(): Unit = batch.close()
    }
  }
}

class PartitionAwareDataSource extends TestingV2Source {

  class MyScanBuilder extends SimpleScanBuilder
    with SupportsReportPartitioning {

    override def planInputPartitions(): Array[InputPartition] = {
      // Note that we don't have same value of column `i` across partitions.
      Array(
        SpecificInputPartition(Array(1, 1, 3), Array(4, 4, 6)),
        SpecificInputPartition(Array(2, 4, 4), Array(6, 2, 2)))
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      SpecificReaderFactory
    }

    override def outputPartitioning(): Partitioning =
      new KeyGroupedPartitioning(Array(FieldReference("i")), 2)
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder()
    }
  }
}

class OrderAndPartitionAwareDataSource extends PartitionAwareDataSource {

  class MyScanBuilder(
      val partitionKeys: Option[Seq[String]],
      val orderKeys: Seq[String])
    extends SimpleScanBuilder
    with SupportsReportPartitioning with SupportsReportOrdering {

    override def planInputPartitions(): Array[InputPartition] = {
      // data are partitioned by column `i` or `j`, so we can report any partitioning
      // column `i` is not ordered globally, but within partitions, together with`j`
      // this allows us to report ordering by [i] and [i, j]
      Array(
        SpecificInputPartition(Array(1, 1, 3), Array(4, 5, 5)),
        SpecificInputPartition(Array(2, 4, 4), Array(6, 1, 2)))
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      SpecificReaderFactory
    }

    override def outputPartitioning(): Partitioning = {
      partitionKeys.map(keys =>
        new KeyGroupedPartitioning(keys.map(FieldReference(_)).toArray, 2)
      ).getOrElse(
        new UnknownPartitioning(2)
      )
    }

    override def outputOrdering(): Array[SortOrder] = orderKeys.map(
      new MySortOrder(_)
    ).toArray
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder(
        Option(options.get("partitionKeys")).map(_.split(",").toImmutableArraySeq),
        Option(options.get("orderKeys")).map(_.split(",").toSeq).getOrElse(Seq.empty)
      )
    }
  }

  class MySortOrder(columnName: String) extends SortOrder {
    override def expression(): Expression = new MyIdentityTransform(
      new MyNamedReference(columnName)
    )
    override def direction(): SortDirection = SortDirection.ASCENDING
    override def nullOrdering(): NullOrdering = NullOrdering.NULLS_FIRST
  }

  class MyNamedReference(parts: String*) extends NamedReference {
    override def fieldNames(): Array[String] = parts.toArray
  }

  class MyIdentityTransform(namedReference: NamedReference) extends Transform {
    override def name(): String = "identity"
    override def references(): Array[NamedReference] = Array.empty
    override def arguments(): Array[Expression] = Seq(namedReference).toArray
  }
}

case class SpecificInputPartition(
    i: Array[Int],
    j: Array[Int]) extends InputPartition with HasPartitionKey {
  override def partitionKey(): InternalRow = PartitionInternalRow(Seq(i(0)).toArray)
}

object SpecificReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[SpecificInputPartition]
    new PartitionReader[InternalRow] {
      private var current = -1

      override def next(): Boolean = {
        current += 1
        current < p.i.length
      }

      override def get(): InternalRow = InternalRow(p.i(current), p.j(current))

      override def close(): Unit = {}
    }
  }
}

class SchemaReadAttemptException(m: String) extends RuntimeException(m)

class SimpleWriteOnlyDataSource extends SimpleWritableDataSource {

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new MyTable(options) {
      override def schema(): StructType = {
        throw new SchemaReadAttemptException("schema should not be read.")
      }
    }
  }
}

/**
 * A writable data source that supports external metadata with a fixed schema (i int, j int).
 */
class WritableDataSourceSupportsExternalMetadata extends SimpleWritableDataSource {
  override def supportsExternalMetadata(): Boolean = true
}

/**
 * A writable data source that supports external metadata with
 * user-specified schema and partitioning.
 */
class CustomSchemaAndPartitioningDataSource extends WritableDataSourceSupportsExternalMetadata {
  class TestTable(
      schema: StructType,
      partitioning: Array[Transform],
      options: CaseInsensitiveStringMap) extends MyTable(options) {
    override def schema(): StructType = schema

    override def partitioning(): Array[Transform] = partitioning
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new TestTable(schema, partitioning, new CaseInsensitiveStringMap(properties))
  }
}

class SupportsExternalMetadataWritableDataSource extends SimpleWritableDataSource {
  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new IllegalArgumentException(
      "Dataframe writer should not require inferring table schema the data source supports" +
        " external metadata.")
  }
}

class ReportStatisticsDataSource extends SimpleWritableDataSource {

  class MyScanBuilder extends SimpleScanBuilder
    with SupportsReportStatistics {
    override def estimateStatistics(): Statistics = {
      new Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.of(80)

        override def numRows(): OptionalLong = OptionalLong.of(10)
      }
    }

    override def planInputPartitions(): Array[InputPartition] = {
      Array(RangeInputPartition(0, 5), RangeInputPartition(5, 10))
    }
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new SimpleBatchTable {
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new MyScanBuilder
      }
    }
  }
}

class InvalidDataSource extends TestingV2Source {
  throw new IllegalArgumentException("test error")

  override def getTable(options: CaseInsensitiveStringMap): Table = null
}

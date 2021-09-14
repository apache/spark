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

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

class DataSourceV2Suite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def getBatch(query: DataFrame): AdvancedBatch = {
    query.queryExecution.executedPlan.collect {
      case d: BatchScanExec =>
        d.batch.asInstanceOf[AdvancedBatch]
    }.head
  }

  private def getJavaBatch(query: DataFrame): JavaAdvancedDataSourceV2.AdvancedBatch = {
    query.queryExecution.executedPlan.collect {
      case d: BatchScanExec =>
        d.batch.asInstanceOf[JavaAdvancedDataSourceV2.AdvancedBatch]
    }.head
  }

  test("simplest implementation") {
    Seq(classOf[SimpleDataSourceV2], classOf[JavaSimpleDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter('i > 5), (6 until 10).map(i => Row(i, -i)))
      }
    }
  }

  test("advanced implementation") {
    Seq(classOf[AdvancedDataSourceV2], classOf[JavaAdvancedDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))

        val q1 = df.select('j)
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

        val q2 = df.filter('i > 3)
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

        val q3 = df.select('i).filter('i > 6)
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

        val q4 = df.select('j).filter('j < -10)
        checkAnswer(q4, Nil)
        if (cls == classOf[AdvancedDataSourceV2]) {
          val batch = getBatch(q4)
          // 'j < 10 is not supported by the testing data source.
          assert(batch.filters.isEmpty)
          assert(batch.requiredSchema.fieldNames === Seq("j"))
        } else {
          val batch = getJavaBatch(q4)
          // 'j < 10 is not supported by the testing data source.
          assert(batch.filters.isEmpty)
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
        checkAnswer(df.select('j), (0 until 90).map(i => Row(-i)))
        checkAnswer(df.filter('i > 50), (51 until 90).map(i => Row(i, -i)))
      }
    }
  }

  test("schema required data source") {
    Seq(classOf[SchemaRequiredDataSource], classOf[JavaSchemaRequiredDataSource]).foreach { cls =>
      withClue(cls.getName) {
        val e = intercept[IllegalArgumentException](spark.read.format(cls.getName).load())
        assert(e.getMessage.contains("requires a user-supplied schema"))

        val schema = new StructType().add("i", "int").add("s", "string")
        val df = spark.read.format(cls.getName).schema(schema).load()

        assert(df.schema == schema)
        assert(df.collect().isEmpty)
      }
    }
  }

  test("SPARK-33369: Skip schema inference in DataframeWriter.save() if table provider " +
    "supports external metadata") {
    withTempDir { dir =>
      val cls = classOf[SupportsExternalMetadataWritableDataSource].getName
      spark.range(10).select('id as 'i, -'id as 'j).write.format(cls)
          .option("path", dir.getCanonicalPath).mode("append").save()
      val schema = new StructType().add("i", "long").add("j", "long")
        checkAnswer(
          spark.read.format(cls).option("path", dir.getCanonicalPath).schema(schema).load(),
          spark.range(10).select('id, -'id))
    }
  }

  test("partitioning reporting") {
    import org.apache.spark.sql.functions.{count, sum}
    Seq(classOf[PartitionAwareDataSource], classOf[JavaPartitionAwareDataSource]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, Seq(Row(1, 4), Row(1, 4), Row(3, 6), Row(2, 6), Row(4, 2), Row(4, 2)))

        val groupByColA = df.groupBy('i).agg(sum('j))
        checkAnswer(groupByColA, Seq(Row(1, 8), Row(2, 6), Row(3, 6), Row(4, 4)))
        assert(collectFirst(groupByColA.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => e
        }.isEmpty)

        val groupByColAB = df.groupBy('i, 'j).agg(count("*"))
        checkAnswer(groupByColAB, Seq(Row(1, 4, 2), Row(2, 6, 1), Row(3, 6, 1), Row(4, 2, 2)))
        assert(collectFirst(groupByColAB.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => e
        }.isEmpty)

        val groupByColB = df.groupBy('j).agg(sum('i))
        checkAnswer(groupByColB, Seq(Row(2, 8), Row(4, 2), Row(6, 5)))
        assert(collectFirst(groupByColB.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => e
        }.isDefined)

        val groupByAPlusB = df.groupBy('i + 'j).agg(count("*"))
        checkAnswer(groupByAPlusB, Seq(Row(5, 2), Row(6, 2), Row(8, 1), Row(9, 1)))
        assert(collectFirst(groupByAPlusB.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => e
        }.isDefined)
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

  test("simple writable data source") {
    Seq(classOf[SimpleWritableDataSource], classOf[JavaSimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        spark.range(10).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).mode("append").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select('id, -'id))

        // default save mode is ErrorIfExists
        intercept[AnalysisException] {
          spark.range(10).select('id as 'i, -'id as 'j).write.format(cls.getName)
            .option("path", path).save()
        }
        spark.range(10).select('id as 'i, -'id as 'j).write.mode("append").format(cls.getName)
          .option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).union(spark.range(10)).select('id, -'id))

        spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).mode("overwrite").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))

        val e = intercept[AnalysisException] {
          spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
            .option("path", path).mode("ignore").save()
        }
        assert(e.message.contains("please use Append or Overwrite modes instead"))

        val e2 = intercept[AnalysisException] {
          spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
            .option("path", path).mode("error").save()
        }
        assert(e2.getMessage.contains("please use Append or Overwrite modes instead"))

        // test transaction
        val failingUdf = org.apache.spark.sql.functions.udf {
          var count = 0
          (id: Long) => {
            if (count > 5) {
              throw new RuntimeException("testing error")
            }
            count += 1
            id
          }
        }
        // this input data will fail to read middle way.
        val input = spark.range(15).select(failingUdf('id).as('i)).select('i, -'i as 'j)
        val e3 = intercept[SparkException] {
          input.write.format(cls.getName).option("path", path).mode("overwrite").save()
        }
        assert(e3.getMessage.contains("Writing job aborted"))
        assert(e3.getErrorClass == "WRITING_JOB_ABORTED")
        assert(e3.getSqlState == "40000")
        // make sure we don't have partial data.
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)
      }
    }
  }

  test("simple counter in writer with onDataWriterCommit") {
    Seq(classOf[SimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        val numPartition = 6
        spark.range(0, 10, 1, numPartition).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .mode("append").option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select('id, -'id))

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

    val q1 = df.select('i + 1)
    checkAnswer(q1, (1 until 11).map(i => Row(i)))
    val batch1 = getBatch(q1)
    assert(batch1.requiredSchema.fieldNames === Seq("i"))

    val q2 = df.select(lit(1))
    checkAnswer(q2, (0 until 10).map(i => Row(1)))
    val batch2 = getBatch(q2)
    assert(batch2.requiredSchema.isEmpty)

    // 'j === 1 can't be pushed down, but we should still be able do column pruning
    val q3 = df.filter('j === -1).select('j * 2)
    checkAnswer(q3, Row(-2))
    val batch3 = getBatch(q3)
    assert(batch3.filters.isEmpty)
    assert(batch3.requiredSchema.fieldNames === Seq("j"))

    // column pruning should work with other operators.
    val q4 = df.sort('i).limit(1).select('i + 1)
    checkAnswer(q4, Row(1))
    val batch4 = getBatch(q4)
    assert(batch4.requiredSchema.fieldNames === Seq("i"))
  }

  test("SPARK-23315: get output from canonicalized data source v2 related plans") {
    def checkCanonicalizedOutput(
        df: DataFrame, logicalNumOutput: Int, physicalNumOutput: Int): Unit = {
      val logical = df.queryExecution.logical.collect {
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
    checkCanonicalizedOutput(df.select('i), 2, 1)
  }

  test("SPARK-25425: extra options should override sessions options during reading") {
    val prefix = "spark.datasource.userDefinedDataSource."
    val optionName = "optionA"
    withSQLConf(prefix + optionName -> "true") {
      val df = spark
        .read
        .option(optionName, false)
        .format(classOf[DataSourceV2WithSessionConfig].getName).load()
      val options = df.queryExecution.logical.collectFirst {
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
      val df = t2.where("i < (select max(a) from t1)").select('i)
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
        val q1 = df.select('i).filter('i > 6)
        val q2 = df.select('i).filter('i > 5)
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
        df.select('i).where("i in (1, null)").collect()
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

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(BATCH_READ)
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
      properties: util.Map[String, String]): Table = {
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
    override def planInputPartitions(): Array[InputPartition] = Array.empty

    override def readSchema(): StructType = schema
  }

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new IllegalArgumentException("requires a user-supplied schema")
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val userGivenSchema = schema
    new SimpleBatchTable {
      override def schema(): StructType = userGivenSchema

      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new MyScanBuilder(userGivenSchema)
      }
    }
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
    throw new UnsupportedOperationException
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
    with SupportsReportPartitioning{

    override def planInputPartitions(): Array[InputPartition] = {
      // Note that we don't have same value of column `a` across partitions.
      Array(
        SpecificInputPartition(Array(1, 1, 3), Array(4, 4, 6)),
        SpecificInputPartition(Array(2, 4, 4), Array(6, 2, 2)))
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      SpecificReaderFactory
    }

    override def outputPartitioning(): Partitioning = new MyPartitioning
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder()
    }
  }

  class MyPartitioning extends Partitioning {
    override def numPartitions(): Int = 2

    override def satisfy(distribution: Distribution): Boolean = distribution match {
      case c: ClusteredDistribution => c.clusteredColumns.contains("i")
      case _ => false
    }
  }
}

case class SpecificInputPartition(i: Array[Int], j: Array[Int]) extends InputPartition

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

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

import java.io.File
import java.util.{ArrayList, List => JList}

import test.org.apache.spark.sql.sources.v2._

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.partitioning.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class DataSourceV2Suite extends QueryTest with SharedSQLContext {
  import testImplicits._

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
    def getReader(query: DataFrame): AdvancedDataSourceV2#Reader = {
      query.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d.reader.asInstanceOf[AdvancedDataSourceV2#Reader]
      }.head
    }

    def getJavaReader(query: DataFrame): JavaAdvancedDataSourceV2#Reader = {
      query.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d.reader.asInstanceOf[JavaAdvancedDataSourceV2#Reader]
      }.head
    }

    Seq(classOf[AdvancedDataSourceV2], classOf[JavaAdvancedDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))

        val q1 = df.select('j)
        checkAnswer(q1, (0 until 10).map(i => Row(-i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val reader = getReader(q1)
          assert(reader.filters.isEmpty)
          assert(reader.requiredSchema.fieldNames === Seq("j"))
        } else {
          val reader = getJavaReader(q1)
          assert(reader.filters.isEmpty)
          assert(reader.requiredSchema.fieldNames === Seq("j"))
        }

        val q2 = df.filter('i > 3)
        checkAnswer(q2, (4 until 10).map(i => Row(i, -i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val reader = getReader(q2)
          assert(reader.filters.flatMap(_.references).toSet == Set("i"))
          assert(reader.requiredSchema.fieldNames === Seq("i", "j"))
        } else {
          val reader = getJavaReader(q2)
          assert(reader.filters.flatMap(_.references).toSet == Set("i"))
          assert(reader.requiredSchema.fieldNames === Seq("i", "j"))
        }

        val q3 = df.select('i).filter('i > 6)
        checkAnswer(q3, (7 until 10).map(i => Row(i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val reader = getReader(q3)
          assert(reader.filters.flatMap(_.references).toSet == Set("i"))
          assert(reader.requiredSchema.fieldNames === Seq("i"))
        } else {
          val reader = getJavaReader(q3)
          assert(reader.filters.flatMap(_.references).toSet == Set("i"))
          assert(reader.requiredSchema.fieldNames === Seq("i"))
        }

        val q4 = df.select('j).filter('j < -10)
        checkAnswer(q4, Nil)
        if (cls == classOf[AdvancedDataSourceV2]) {
          val reader = getReader(q4)
          // 'j < 10 is not supported by the testing data source.
          assert(reader.filters.isEmpty)
          assert(reader.requiredSchema.fieldNames === Seq("j"))
        } else {
          val reader = getJavaReader(q4)
          // 'j < 10 is not supported by the testing data source.
          assert(reader.filters.isEmpty)
          assert(reader.requiredSchema.fieldNames === Seq("j"))
        }
      }
    }
  }

  test("columnar batch scan implementation") {
    Seq(classOf[BatchDataSourceV2], classOf[JavaBatchDataSourceV2]).foreach { cls =>
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

  test("partitioning reporting") {
    import org.apache.spark.sql.functions.{count, sum}
    Seq(classOf[PartitionAwareDataSource], classOf[JavaPartitionAwareDataSource]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, Seq(Row(1, 4), Row(1, 4), Row(3, 6), Row(2, 6), Row(4, 2), Row(4, 2)))

        val groupByColA = df.groupBy('a).agg(sum('b))
        checkAnswer(groupByColA, Seq(Row(1, 8), Row(2, 6), Row(3, 6), Row(4, 4)))
        assert(groupByColA.queryExecution.executedPlan.collectFirst {
          case e: ShuffleExchangeExec => e
        }.isEmpty)

        val groupByColAB = df.groupBy('a, 'b).agg(count("*"))
        checkAnswer(groupByColAB, Seq(Row(1, 4, 2), Row(2, 6, 1), Row(3, 6, 1), Row(4, 2, 2)))
        assert(groupByColAB.queryExecution.executedPlan.collectFirst {
          case e: ShuffleExchangeExec => e
        }.isEmpty)

        val groupByColB = df.groupBy('b).agg(sum('a))
        checkAnswer(groupByColB, Seq(Row(2, 8), Row(4, 2), Row(6, 5)))
        assert(groupByColB.queryExecution.executedPlan.collectFirst {
          case e: ShuffleExchangeExec => e
        }.isDefined)

        val groupByAPlusB = df.groupBy('a + 'b).agg(count("*"))
        checkAnswer(groupByAPlusB, Seq(Row(5, 2), Row(6, 2), Row(8, 1), Row(9, 1)))
        assert(groupByAPlusB.queryExecution.executedPlan.collectFirst {
          case e: ShuffleExchangeExec => e
        }.isDefined)
      }
    }
  }

  test("SPARK-23574: no shuffle exchange with single partition") {
    val df = spark.read.format(classOf[SimpleSinglePartitionSource].getName).load().agg(count("*"))
    assert(df.queryExecution.executedPlan.collect { case e: Exchange => e }.isEmpty)
  }

  test("simple writable data source") {
    // TODO: java implementation.
    Seq(classOf[SimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        spark.range(10).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select('id, -'id))

        // test with different save modes
        spark.range(10).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).mode("append").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).union(spark.range(10)).select('id, -'id))

        spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).mode("overwrite").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))

        spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).mode("ignore").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))

        val e = intercept[Exception] {
          spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
            .option("path", path).mode("error").save()
        }
        assert(e.getMessage.contains("data already exists"))

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
        val input = spark.range(10).select(failingUdf('id).as('i)).select('i, -'i as 'j)
        val e2 = intercept[SparkException] {
          input.write.format(cls.getName).option("path", path).mode("overwrite").save()
        }
        assert(e2.getMessage.contains("Writing job aborted"))
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
          .option("path", path).save()
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
    def getReader(query: DataFrame): AdvancedDataSourceV2#Reader = {
      query.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d.reader.asInstanceOf[AdvancedDataSourceV2#Reader]
      }.head
    }

    val df = spark.read.format(classOf[AdvancedDataSourceV2].getName).load()

    val q1 = df.select('i + 1)
    checkAnswer(q1, (1 until 11).map(i => Row(i)))
    val reader1 = getReader(q1)
    assert(reader1.requiredSchema.fieldNames === Seq("i"))

    val q2 = df.select(lit(1))
    checkAnswer(q2, (0 until 10).map(i => Row(1)))
    val reader2 = getReader(q2)
    assert(reader2.requiredSchema.isEmpty)

    // 'j === 1 can't be pushed down, but we should still be able do column pruning
    val q3 = df.filter('j === -1).select('j * 2)
    checkAnswer(q3, Row(-2))
    val reader3 = getReader(q3)
    assert(reader3.filters.isEmpty)
    assert(reader3.requiredSchema.fieldNames === Seq("j"))

    // column pruning should work with other operators.
    val q4 = df.sort('i).limit(1).select('i + 1)
    checkAnswer(q4, Row(1))
    val reader4 = getReader(q4)
    assert(reader4.requiredSchema.fieldNames === Seq("i"))
  }

  test("SPARK-23315: get output from canonicalized data source v2 related plans") {
    def checkCanonicalizedOutput(
        df: DataFrame, logicalNumOutput: Int, physicalNumOutput: Int): Unit = {
      val logical = df.queryExecution.optimizedPlan.collect {
        case d: DataSourceV2Relation => d
      }.head
      assert(logical.canonicalized.output.length == logicalNumOutput)

      val physical = df.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d
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
      val options = df.queryExecution.optimizedPlan.collectFirst {
        case d: DataSourceV2Relation => d.options
      }
      assert(options.get.get(optionName) == Some("false"))
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
          df.write.format(format).option("path", optionPath).save()
          assert(!new File(sessionPath).exists)
          checkAnswer(spark.read.format(format).option("path", optionPath).load(), df)
        }
      }
    }
  }

  test("SPARK-25700: do not read schema when writing") {
    withTempPath { file =>
      val cls = classOf[SimpleWriteOnlyDataSource]
      val path = file.getCanonicalPath
      val df = spark.range(5).select('id as 'i, -'id as 'j)
      try {
        df.write.format(cls.getName).option("path", path).mode("error").save()
        df.write.format(cls.getName).option("path", path).mode("overwrite").save()
        df.write.format(cls.getName).option("path", path).mode("ignore").save()
        df.write.format(cls.getName).option("path", path).mode("append").save()
      } catch {
        case e: SchemaReadAttemptException => fail("Schema read was attempted.", e)
      }
    }
  }

  test("SPARK-32609: DataSourceV2 with different pushedfilters should be different") {
    def getScanExec(query: DataFrame): DataSourceV2ScanExec = {
      query.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d
      }.head
    }

    Seq(classOf[AdvancedDataSourceV2], classOf[JavaAdvancedDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        val q1 = df.select('i).filter('i > 6)
        val q2 = df.select('i).filter('i > 5)
        val q3 = df.select('i).filter('i > 5)
        val scan1 = getScanExec(q1)
        val scan2 = getScanExec(q2)
        val scan3 = getScanExec(q3)
        assert(!scan1.equals(scan2))
        assert(scan2.equals(scan3))
      }
    }
  }

  test("SPARK-32708: same columns with different ExprIds should be equal after canonicalization ") {
    def getV2ScanExec(query: DataFrame): DataSourceV2ScanExec = {
      query.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d
      }.head
    }

    val df1 = spark.read.format(classOf[AdvancedDataSourceV2].getName).load()
    val q1 = df1.select('i).filter('i > 6)
    val df2 = spark.read.format(classOf[AdvancedDataSourceV2].getName).load()
    val q2 = df2.select('i).filter('i > 6)
    val scan1 = getV2ScanExec(q1)
    val scan2 = getV2ScanExec(q2)
    assert(scan1.sameResult(scan2))
    assert(scan1.doCanonicalize().equals(scan2.doCanonicalize()))

    val q3 = df2.select('i).filter('i > 5)
    val scan3 = getV2ScanExec(q3)
    assert(!scan1.sameResult(scan3))
    assert(!scan1.doCanonicalize().equals(scan3.doCanonicalize()))
  }

}

class SimpleSinglePartitionSource extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader {
    override def readSchema(): StructType = new StructType().add("i", "int").add("j", "int")

    override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
      java.util.Arrays.asList(new SimpleInputPartition(0, 5))
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

// This class is used by pyspark tests. If this class is modified/moved, make sure pyspark
// tests still pass.
class SimpleDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader {
    override def readSchema(): StructType = new StructType().add("i", "int").add("j", "int")

    override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
      java.util.Arrays.asList(new SimpleInputPartition(0, 5), new SimpleInputPartition(5, 10))
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class SimpleInputPartition(start: Int, end: Int)
  extends InputPartition[InternalRow]
  with InputPartitionReader[InternalRow] {
  private var current = start - 1

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new SimpleInputPartition(start, end)

  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): InternalRow = InternalRow(current, -current)

  override def close(): Unit = {}
}

class AdvancedDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader
    with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

    var requiredSchema = new StructType().add("i", "int").add("j", "int")
    var filters = Array.empty[Filter]

    override def pruneColumns(requiredSchema: StructType): Unit = {
      this.requiredSchema = requiredSchema
    }

    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      val (supported, unsupported) = filters.partition {
        case GreaterThan("i", _: Int) => true
        case _ => false
      }
      this.filters = supported
      unsupported
    }

    override def pushedFilters(): Array[Filter] = filters

    override def readSchema(): StructType = {
      requiredSchema
    }

    override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
      val lowerBound = filters.collectFirst {
        case GreaterThan("i", v: Int) => v
      }

      val res = new ArrayList[InputPartition[InternalRow]]

      if (lowerBound.isEmpty) {
        res.add(new AdvancedInputPartition(0, 5, requiredSchema))
        res.add(new AdvancedInputPartition(5, 10, requiredSchema))
      } else if (lowerBound.get < 4) {
        res.add(new AdvancedInputPartition(lowerBound.get + 1, 5, requiredSchema))
        res.add(new AdvancedInputPartition(5, 10, requiredSchema))
      } else if (lowerBound.get < 9) {
        res.add(new AdvancedInputPartition(lowerBound.get + 1, 10, requiredSchema))
      }

      res
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class AdvancedInputPartition(start: Int, end: Int, requiredSchema: StructType)
  extends InputPartition[InternalRow] with InputPartitionReader[InternalRow] {

  private var current = start - 1

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new AdvancedInputPartition(start, end, requiredSchema)
  }

  override def close(): Unit = {}

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
}


class SchemaRequiredDataSource extends DataSourceV2 with ReadSupport {

  class Reader(val readSchema: StructType) extends DataSourceReader {
    override def planInputPartitions(): JList[InputPartition[InternalRow]] =
      java.util.Collections.emptyList()
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    throw new IllegalArgumentException("requires a user-supplied schema")
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new Reader(schema)
  }
}

class BatchDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader with SupportsScanColumnarBatch {
    override def readSchema(): StructType = new StructType().add("i", "int").add("j", "int")

    override def planBatchInputPartitions(): JList[InputPartition[ColumnarBatch]] = {
      java.util.Arrays.asList(
        new BatchInputPartitionReader(0, 50), new BatchInputPartitionReader(50, 90))
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class BatchInputPartitionReader(start: Int, end: Int)
  extends InputPartition[ColumnarBatch] with InputPartitionReader[ColumnarBatch] {

  private final val BATCH_SIZE = 20
  private lazy val i = new OnHeapColumnVector(BATCH_SIZE, IntegerType)
  private lazy val j = new OnHeapColumnVector(BATCH_SIZE, IntegerType)
  private lazy val batch = new ColumnarBatch(Array(i, j))

  private var current = start

  override def createPartitionReader(): InputPartitionReader[ColumnarBatch] = this

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

  override def get(): ColumnarBatch = {
    batch
  }

  override def close(): Unit = batch.close()
}

class PartitionAwareDataSource extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader with SupportsReportPartitioning {
    override def readSchema(): StructType = new StructType().add("a", "int").add("b", "int")

    override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
      // Note that we don't have same value of column `a` across partitions.
      java.util.Arrays.asList(
        new SpecificInputPartitionReader(Array(1, 1, 3), Array(4, 4, 6)),
        new SpecificInputPartitionReader(Array(2, 4, 4), Array(6, 2, 2)))
    }

    override def outputPartitioning(): Partitioning = new MyPartitioning
  }

  class MyPartitioning extends Partitioning {
    override def numPartitions(): Int = 2

    override def satisfy(distribution: Distribution): Boolean = distribution match {
      case c: ClusteredDistribution => c.clusteredColumns.contains("a")
      case _ => false
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class SpecificInputPartitionReader(i: Array[Int], j: Array[Int])
  extends InputPartition[InternalRow]
  with InputPartitionReader[InternalRow] {
  assert(i.length == j.length)

  private var current = -1

  override def createPartitionReader(): InputPartitionReader[InternalRow] = this

  override def next(): Boolean = {
    current += 1
    current < i.length
  }

  override def get(): InternalRow = InternalRow(i(current), j(current))

  override def close(): Unit = {}
}

class SchemaReadAttemptException(m: String) extends RuntimeException(m)

class SimpleWriteOnlyDataSource extends SimpleWritableDataSource {
  override def fullSchema(): StructType = {
    // This is a bit hacky since this source implements read support but throws
    // during schema retrieval. Might have to rewrite but it's done
    // such so for minimised changes.
    throw new SchemaReadAttemptException("read is not supported")
  }
}

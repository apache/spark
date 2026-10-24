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
package org.apache.spark.sql.execution.datasources.v2.columnar

import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.{Literal, NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Tests the Data Source V2 adapter of [[ColumnarDataSource]] with a data source implemented on the
 * JVM. `NativeDataSourceSuite` tests the native data sources, which implement it.
 */
class ColumnarDataSourceSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val format = "test_columnar_range"

  private def scan(df: org.apache.spark.sql.classic.DataFrame): ColumnarScan = {
    df.queryExecution.optimizedPlan.collectFirst {
      case relation: DataSourceV2ScanRelation => relation.scan.asInstanceOf[ColumnarScan]
    }.get
  }

  private def options(pairs: (String, String)*): CaseInsensitiveStringMap = {
    new CaseInsensitiveStringMap(pairs.toMap.asJava)
  }

  test("batch read") {
    val df = spark.read.format(format).option("end", "10").load()
    assert(df.schema == StructType.fromDDL("id BIGINT, name STRING"))
    checkAnswer(df, (0L until 10L).map(id => Row(id, s"name$id")))
    // A data source can also be used by the class name of its provider.
    checkAnswer(
      spark.read.format(classOf[TestRangeProvider].getName).load(),
      (0L until 10L).map(id => Row(id, s"name$id")))
    checkAnswer(
      spark.read.schema("id BIGINT").format(format).option("end", "3").load(),
      Seq(Row(0L), Row(1L), Row(2L)))
  }

  test("push down predicates, a limit and the required columns") {
    val filtered = spark.read.format(format).option("end", "10").load().filter("id > 6")
    checkAnswer(filtered, (7L until 10L).map(id => Row(id, s"name$id")))
    assert(scan(filtered).getMetaData()("PushedPredicates").contains("id > 6"))

    val pruned = spark.read.format(format).option("end", "4").load().select("name")
    checkAnswer(pruned, (0L until 4L).map(id => Row(s"name$id")))
    assert(scan(pruned).readSchema() == StructType.fromDDL("name STRING"))

    val limited = spark.read.format(format).option("end", "10").load().limit(2)
    assert(limited.collect().length == 2)
    assert(scan(limited).getMetaData()("PushedLimit") == "LIMIT 2")
  }

  test("tables in a catalog") {
    withTable("columnar_range") {
      sql(s"CREATE TABLE columnar_range USING $format OPTIONS (end 3, table 'catalog_write')")
      // The options of the table are used by the scans of the table, unless a scan sets them.
      checkAnswer(spark.table("columnar_range"), (0L until 3L).map(id => Row(id, s"name$id")))
      checkAnswer(
        spark.read.option("END", "2").table("columnar_range"),
        (0L until 2L).map(id => Row(id, s"name$id")))

      sql("INSERT INTO columnar_range VALUES (1, 'a')")
      assert(TestColumnarSink.rows("catalog_write") == Seq((1L, "a")))
    }
  }

  test("batch write") {
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "3") {
      val table = "batch_write"
      (0L until 7L).map(id => (id, s"v$id")).toDF("id", "name").coalesce(1)
        .write.format(format).option("table", table).mode("append").save()
      assert(TestColumnarSink.rows(table) == (0L until 7L).map(id => (id, s"v$id")))
      // The rows are written in Arrow-backed batches of at most 3 rows.
      assert(TestColumnarSink.batchSizes(table) == Seq(3, 3, 1))

      Seq((10L, "a")).toDF("id", "name")
        .write.format(format).option("table", table).mode("append").save()
      assert(TestColumnarSink.rows(table).length == 8)
      Seq((20L, "b")).toDF("id", "name")
        .write.format(format).option("table", table).mode("overwrite").save()
      assert(TestColumnarSink.rows(table) == Seq((20L, "b")))
    }
  }

  test("batch write in batches of a limited size") {
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_BATCH.key -> "4096") {
      val table = "batch_write_bytes"
      val rows = (0L until 10L).map(id => (id, "x" * 1000 + id))
      rows.toDF("id", "name").coalesce(1)
        .write.format(format).option("table", table).mode("append").save()
      assert(TestColumnarSink.rows(table) == rows)
      // A batch ends once it holds at least 4096 bytes, long before it has 10,000 rows: each row
      // takes about 1 KB, so a batch has 5 rows.
      assert(TestColumnarSink.batchSizes(table) == Seq(5, 5))
    }
  }

  test("create the writer once the data to write is computed") {
    val table = "failed_upstream"
    // The error happens in a shuffle stage, which adaptive query execution runs before the write,
    // so no writer is created. Spark would not abort it.
    val e = intercept[Exception] {
      spark.range(10).selectExpr("id", "if(id >= 0, raise_error('boom'), 'x') AS name")
        .repartition(2)
        .write.format(format).option("table", table).mode("append").save()
    }
    assert(e.getMessage.contains("boom"))
    assert(!TestColumnarSink.writers.contains(table))
    assert(!TestColumnarSink.aborted.contains(table))
  }

  test("the table reports the capabilities of the data source") {
    val table = "unsupported_write"
    // Computing the data would fail, in a shuffle stage that adaptive query execution runs before
    // the write: Data Source V2 rejects the write while it analyzes the query, before that.
    val df = spark.range(10).selectExpr("id", "if(id >= 0, raise_error('boom'), 'x') AS name")
      .repartition(2)
    checkError(
      exception = intercept[AnalysisException] {
        df.write.format(format).option("table", table).option("capabilities", "BATCH_READ")
          .mode("append").save()
      },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map("tableName" -> s"`$format`", "operation" -> "batch write"))
    checkError(
      exception = intercept[AnalysisException] {
        df.write.format(format).option("table", table)
          .option("capabilities", "BATCH_READ, BATCH_WRITE").mode("overwrite").save()
      },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map("tableName" -> s"`$format`", "operation" -> "truncate in batch mode"))
    assert(!TestColumnarSink.writers.contains(table))
  }

  test("build and plan a batch scan once") {
    val dataSource = new TestRangeDataSource(options("end" -> "4"))
    val builder = new ColumnarScanBuilder(format, dataSource.schema(), () => dataSource)
    val scan = builder.build()
    assert(builder.build() eq scan)
    val batch = scan.toBatch
    assert(scan.toBatch eq batch)
    val partitions = batch.planInputPartitions()
    assert(partitions.length == 2)
    assert(batch.planInputPartitions() eq partitions)
    assert(batch.createReaderFactory() eq batch.createReaderFactory())
    assert(dataSource.plans.get() == 1)

    // A failure is planned once too, and Spark gets the same error each time it asks.
    val failing = new TestRangeDataSource(options("failPlanning" -> "true"))
    val failingBatch =
      new ColumnarScanBuilder(format, failing.schema(), () => failing).build().toBatch
    val e = intercept[IllegalStateException](failingBatch.planInputPartitions())
    assert(e.getMessage == "injected planning failure")
    assert(intercept[IllegalStateException](failingBatch.createReaderFactory()) eq e)
    assert(failing.plans.get() == 1)
  }

  test("abort a failed write") {
    val table = "failed_write"
    val e = intercept[SparkException] {
      Seq((1L, "a")).toDF("id", "name").write.format(format).option("table", table)
        .option("fail", "true").mode("append").save()
    }
    assert(Utils.exceptionString(e).contains("injected failure"))
    assert(TestColumnarSink.aborted.contains(table))
    assert(!TestColumnarSink.committed.containsKey(table))
  }

  test("streaming read and write") {
    val table = "streaming_write"
    val query = spark.readStream.format(format).option("end", "5").load()
      .writeStream
      .format(format)
      .option("table", table)
      .option("checkpointLocation", Utils.createTempDir().getPath)
      .start()
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }
    assert(TestColumnarSink.rows(table).sorted == (0L until 5L).map(id => (id, s"name$id")))
    assert(TestColumnarSink.epochs.get(table) == Seq(0L))
  }

  test("create a stream writer for each micro-batch") {
    val table = "micro_batch_write"
    val input = MemoryStream[(Long, String)]
    val query = input.toDF().toDF("id", "name")
      .writeStream
      .format(format)
      .option("table", table)
      .option("checkpointLocation", Utils.createTempDir().getPath)
      .start()
    try {
      (0L until 3L).foreach { id =>
        input.addData((id, s"name$id"))
        query.processAllAvailable()
      }
    } finally {
      query.stop()
    }
    assert(TestColumnarSink.rows(table).sorted == (0L until 3L).map(id => (id, s"name$id")))
    assert(TestColumnarSink.epochs.get(table) == Seq(0L, 1L, 2L))
    // Each micro-batch creates its stream writer with the ID of the query, so that the native
    // data sources close the stream writer of the previous micro-batch of the query.
    assert(TestColumnarSink.streamWriterQueryIds.get(table) == Seq.fill(3)(query.id.toString))
  }
}

/** Reads the ids [0, end) and their names, and writes to [[TestColumnarSink]]. */
class TestRangeProvider extends TableProvider with DataSourceRegister {
  override def shortName(): String = "test_columnar_range"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    new TestRangeDataSource(options).schema()
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new ColumnarTable(shortName(), schema, properties, new TestRangeDataSource(_))
  }

  override def supportsExternalMetadata(): Boolean = true
}

class TestRangeDataSource(options: CaseInsensitiveStringMap) extends ColumnarDataSource {
  private val end = options.getLong("end", 10)
  private val table = options.getOrDefault("table", "default")
  // The number of batch scans that the planners of this data source planned.
  val plans = new AtomicInteger()

  override def schema(): StructType = StructType.fromDDL("id BIGINT, name STRING")

  // The names of the capabilities, such as "BATCH_READ, BATCH_WRITE", or all of them by default.
  override def capabilities(): util.Set[TableCapability] = {
    Option(options.get("capabilities")).map { names =>
      val capabilities = util.EnumSet.noneOf(classOf[TableCapability])
      names.split(",").map(_.trim).foreach(name => capabilities.add(TableCapability.valueOf(name)))
      capabilities
    }.getOrElse(super.capabilities())
  }

  override def batchPlanner(schema: StructType): ColumnarBatchPlanner = {
    new TestRangePlanner(end, schema, options.getBoolean("failPlanning", false), plans)
  }

  override def streamReader(schema: StructType): ColumnarStreamReader = {
    new TestRangeStreamReader(end, schema)
  }

  override def writer(schema: StructType, overwrite: Boolean): ColumnarWriter = {
    TestColumnarSink.writers.add(table)
    new TestSinkWriter(table, overwrite, options.getBoolean("fail", false))
  }

  override def streamWriter(
      schema: StructType,
      overwrite: Boolean,
      queryId: String): ColumnarStreamWriter = {
    TestColumnarSink.streamWriterQueryIds.compute(
      table, (_, old) => Option(old).getOrElse(Nil) :+ queryId)
    new TestSinkStreamWriter(table)
  }
}

case class TestRangePartition(lo: Long, hi: Long) extends InputPartition

/** Accepts the predicates `id > <literal>`. */
class TestRangePlanner(end: Long, schema: StructType, fail: Boolean, plans: AtomicInteger)
  extends ColumnarBatchPlanner {
  private var lo = 0L
  private var limit = Long.MaxValue
  private var readSchema = schema

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    predicates.filter { predicate =>
      (predicate.name(), predicate.children()) match {
        case (">", Array(column: NamedReference, literal: Literal[_]))
            if column.fieldNames().sameElements(Array("id")) =>
          lo = math.max(lo, literal.value().asInstanceOf[Long] + 1)
          true
        case _ => false
      }
    }
  }

  override def pushLimit(limit: Int): Boolean = {
    this.limit = limit
    true
  }

  override def pruneColumns(requiredSchema: StructType): Boolean = {
    readSchema = requiredSchema
    true
  }

  override def plan(): ColumnarPlannedBatch = {
    plans.incrementAndGet()
    if (fail) {
      throw new IllegalStateException("injected planning failure")
    }
    val hi = if (limit == Long.MaxValue) end else math.min(end, lo + limit)
    val mid = (lo + hi) / 2
    ColumnarPlannedBatch(
      Array(TestRangePartition(lo, mid), TestRangePartition(mid, hi)),
      new TestRangeReaderFactory(readSchema))
  }
}

class TestRangeReaderFactory(readSchema: StructType) extends ColumnarOnlyPartitionReaderFactory {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val range = partition.asInstanceOf[TestRangePartition]
    new TestBatchReader(range.lo, range.hi, readSchema)
  }
}

class TestRangeStreamReader(end: Long, schema: StructType) extends ColumnarStreamReader {
  private def offset(json: String): Long = json.stripPrefix("{\"offset\":").stripSuffix("}").toLong

  override def initialOffset(): String = "{\"offset\":0}"

  override def latestOffset(): String = s"""{"offset":$end}"""

  override def partitions(start: String, end: String): Array[InputPartition] = {
    Array(TestRangePartition(offset(start), offset(end)))
  }

  override def read(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val range = partition.asInstanceOf[TestRangePartition]
    new TestBatchReader(range.lo, range.hi, schema)
  }
}

/** Returns the ids [lo, hi) and their names in a single batch. */
class TestBatchReader(lo: Long, hi: Long, schema: StructType)
  extends PartitionReader[ColumnarBatch] {
  private var consumed = false
  private lazy val batch = {
    val numRows = (hi - lo).toInt
    val vectors = OnHeapColumnVector.allocateColumns(math.max(numRows, 1), schema)
    for (i <- 0 until numRows; (name, column) <- schema.fieldNames.zipWithIndex) {
      name match {
        case "id" => vectors(column).putLong(i, lo + i)
        case "name" => vectors(column).putByteArray(i, s"name${lo + i}".getBytes(UTF_8))
      }
    }
    new ColumnarBatch(vectors.map(v => v: ColumnVector), numRows)
  }

  override def next(): Boolean = {
    val hasNext = !consumed
    consumed = true
    hasNext
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = if (consumed) batch.close()
}

/** The data written by the test writers, by table. */
object TestColumnarSink {
  val committed = new ConcurrentHashMap[String, Seq[TestCommitMessage]]()
  val aborted = ConcurrentHashMap.newKeySet[String]()
  // The tables for which a writer was created.
  val writers = ConcurrentHashMap.newKeySet[String]()
  val epochs = new ConcurrentHashMap[String, Seq[Long]]()
  val streamWriterQueryIds = new ConcurrentHashMap[String, Seq[String]]()

  def rows(table: String): Seq[(Long, String)] = committed.get(table).flatMap(_.rows)

  def batchSizes(table: String): Seq[Int] = committed.get(table).flatMap(_.batchSizes)

  def commit(table: String, messages: Array[WriterCommitMessage], overwrite: Boolean): Unit = {
    val newMessages = messages.map(_.asInstanceOf[TestCommitMessage]).toSeq
    committed.compute(table, (_, old) => {
      if (old == null || overwrite) newMessages else old ++ newMessages
    })
  }
}

case class TestCommitMessage(rows: Seq[(Long, String)], batchSizes: Seq[Int])
  extends WriterCommitMessage

class TestDataWriter(fail: Boolean) extends DataWriter[ColumnarBatch] {
  private val rows = ArrayBuffer.empty[(Long, String)]
  private val batchSizes = ArrayBuffer.empty[Int]

  override def write(batch: ColumnarBatch): Unit = {
    if (fail) {
      throw new IllegalStateException("injected failure")
    }
    assert((0 until batch.numCols()).forall(batch.column(_).isInstanceOf[ArrowColumnVector]))
    batchSizes += batch.numRows()
    (0 until batch.numRows()).foreach { i =>
      rows += ((batch.column(0).getLong(i), batch.column(1).getUTF8String(i).toString))
    }
  }

  override def commit(): WriterCommitMessage = TestCommitMessage(rows.toSeq, batchSizes.toSeq)

  override def abort(): Unit = {}

  override def close(): Unit = {}
}

class TestSinkWriter(table: String, overwrite: Boolean, fail: Boolean) extends ColumnarWriter {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    new TestDataWriter(fail)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    TestColumnarSink.commit(table, messages, overwrite)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    TestColumnarSink.aborted.add(table)
  }
}

class TestSinkStreamWriter(table: String) extends ColumnarStreamWriter {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = new TestDataWriter(fail = false)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    TestColumnarSink.commit(table, messages, overwrite = false)
    TestColumnarSink.epochs.compute(table, (_, old) => Option(old).getOrElse(Nil) :+ epochId)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    TestColumnarSink.aborted.add(table)
  }
}

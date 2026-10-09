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
package org.apache.spark.sql.datasource

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkException, SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.expressions.{Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.columnar.ColumnarScan
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

class ColumnarDataSourceSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val format = "test_columnar_range"

  private def scan(df: org.apache.spark.sql.classic.DataFrame): ColumnarScan = {
    df.queryExecution.optimizedPlan.collectFirst {
      case relation: DataSourceV2ScanRelation => relation.scan.asInstanceOf[ColumnarScan]
    }.get
  }

  private def findError(e: Throwable, condition: String): SparkThrowable = {
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null)
      .collectFirst { case t: SparkThrowable if t.getCondition == condition => t }
      .getOrElse(throw e)
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

  test("operations that the data source does not implement") {
    val provider = classOf[TestEmptyProvider].getName
    val description = classOf[TestEmptyDataSource].getName
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        spark.read.format(provider).load()
      },
      condition = "UNABLE_TO_INFER_SCHEMA",
      parameters = Map("format" -> description))
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        spark.read.schema("id BIGINT").format(provider).load().collect()
      },
      condition = "DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED",
      parameters = Map("description" -> description))
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        spark.range(1).write.format(provider).mode("append").save()
      },
      condition = "DATA_SOURCE_BATCH_WRITE_NOT_SUPPORTED",
      parameters = Map("description" -> description))

    def checkStreaming(condition: String)(start: => StreamingQuery): Unit = {
      val e = intercept[Exception] {
        val query = start
        try query.processAllAvailable() finally query.stop()
      }
      checkError(
        exception = findError(e, condition),
        condition = condition,
        parameters = Map("description" -> description))
    }
    checkStreaming("DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED") {
      spark.readStream.schema("id BIGINT").format(provider).load()
        .writeStream.format("noop").start()
    }
    checkStreaming("DATA_SOURCE_STREAMING_WRITE_NOT_SUPPORTED") {
      spark.readStream.format("rate").load().writeStream.format(provider)
        .option("checkpointLocation", Utils.createTempDir().getPath).start()
    }
  }
}

/** Reads the ids [0, end) and their names, and writes to [[TestColumnarSink]]. */
class TestRangeProvider extends DataSourceProvider {
  override def shortName(): String = "test_columnar_range"

  override def createDataSource(options: CaseInsensitiveStringMap): DataSource = {
    new TestRangeDataSource(options)
  }
}

class TestRangeDataSource(options: CaseInsensitiveStringMap) extends DataSource {
  private val end = options.getLong("end", 10)
  private val table = options.getOrDefault("table", "default")

  override def schema(): StructType = StructType.fromDDL("id BIGINT, name STRING")

  override def reader(schema: StructType): DataSourceReader = new TestRangeReader(end, schema)

  override def streamReader(schema: StructType): DataSourceStreamReader = {
    new TestRangeStreamReader(end, schema)
  }

  override def writer(schema: StructType, overwrite: Boolean): DataSourceWriter = {
    new TestSinkWriter(table, overwrite, options.getBoolean("fail", false))
  }

  override def streamWriter(schema: StructType, overwrite: Boolean): DataSourceStreamWriter = {
    new TestSinkStreamWriter(table)
  }
}

case class TestRangePartition(lo: Long, hi: Long) extends InputPartition

/** Accepts the predicates `id > <literal>`. */
class TestRangeReader(end: Long, schema: StructType) extends DataSourceReader {
  private var lo = 0L
  private var limit = Long.MaxValue
  private var readSchema = schema

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    predicates.filterNot { predicate =>
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

  override def partitions(): Array[InputPartition] = {
    val hi = if (limit == Long.MaxValue) end else math.min(end, lo + limit)
    val mid = (lo + hi) / 2
    Array(TestRangePartition(lo, mid), TestRangePartition(mid, hi))
  }

  override def read(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val range = partition.asInstanceOf[TestRangePartition]
    new TestBatchReader(range.lo, range.hi, readSchema)
  }
}

class TestRangeStreamReader(end: Long, schema: StructType) extends DataSourceStreamReader {
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
  val epochs = new ConcurrentHashMap[String, Seq[Long]]()

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

class TestSinkWriter(table: String, overwrite: Boolean, fail: Boolean) extends DataSourceWriter {
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

class TestSinkStreamWriter(table: String) extends DataSourceStreamWriter {
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

/** A data source that implements nothing. */
class TestEmptyProvider extends DataSourceProvider {
  override def shortName(): String = "test_columnar_empty"

  override def createDataSource(options: CaseInsensitiveStringMap): DataSource = {
    new TestEmptyDataSource
  }
}

class TestEmptyDataSource extends DataSource

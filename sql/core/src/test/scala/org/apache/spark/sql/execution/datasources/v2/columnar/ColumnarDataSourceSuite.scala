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
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.vector.{BigIntVector, DecimalVector, FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.{SparkArithmeticException, SparkException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnConverter, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, MicroBatchScanExec, V2TableWriteExec}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, ReusedArrowColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{ArrowUtils, CaseInsensitiveStringMap}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Tests the Data Source V2 adapter of [[ColumnarDataSource]] with a data source implemented on the
 * JVM. `NativeDataSourceSuite` tests the native data sources, which implement it.
 */
class ColumnarDataSourceSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {
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

  test("push down a limit and the required columns") {
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

  test("write the columnar batches of a query") {
    // Runs a write, and returns the plan of the query that it writes.
    def writtenPlan(write: => Unit): SparkPlan = {
      QueryTest.withQueryExecutionsCaptured(spark)(write).flatMap { qe =>
        qe.executedPlan.collectFirst { case writeExec: V2TableWriteExec => writeExec.query }
      }.head
    }
    // Writes the ids [0, 10) and their names, read in Arrow batches of `arrowBatchSize` rows, or in
    // a batch of on-heap vectors for each of the two partitions of the scan.
    def copy(table: String, arrowBatchSize: Int = 0, keep: Boolean = false): SparkPlan = {
      writtenPlan {
        spark.read.format(format).option("end", "10").option("arrowBatchSize", arrowBatchSize)
          .load()
          .write.format(format).option("table", table).option("keep", keep).mode("append").save()
      }
    }
    val expected = (0L until 10L).map(id => (id, s"name$id"))

    withSQLConf(
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "3",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "false") {
      // The data writers get the batches of the scan, without a transition to rows. On-heap
      // vectors are converted to Arrow value by value, into batches that end as those of rows do.
      assert(copy("columnar_write").isInstanceOf[BatchScanExec])
      assert(TestColumnarSink.rows("columnar_write") == expected)
      assert(TestColumnarSink.batchSizes("columnar_write") == Seq(3, 2, 3, 2))

      // Arrow batches are copied buffer by buffer, each in batches of at most 3 rows.
      assert(copy("arrow_write", arrowBatchSize = 2).isInstanceOf[BatchScanExec])
      assert(TestColumnarSink.rows("arrow_write") == expected)
      assert(TestColumnarSink.batchSizes("arrow_write") == Seq(2, 2, 1, 2, 2, 1))
      copy("large_arrow_write", arrowBatchSize = 5)
      assert(TestColumnarSink.rows("large_arrow_write") == expected)
      assert(TestColumnarSink.batchSizes("large_arrow_write") == Seq(3, 2, 3, 2))

      // A data writer may keep the data of the batches that it gets, though the scan overwrites
      // its Arrow vectors with each batch.
      copy("kept_arrow_write", arrowBatchSize = 2, keep = true)
      assert(TestColumnarSink.rows("kept_arrow_write") == expected)

      // A query that does not produce columnar batches is written as rows.
      val plan = writtenPlan {
        spark.read.format(format).option("end", "10").load()
          .selectExpr("id", "upper(name) AS name")
          .write.format(format).option("table", "row_write").mode("append").save()
      }
      assert(plan.exists(_.isInstanceOf[ColumnarToRowExec]), plan)

      // With adaptive query execution, a query with a shuffle is an adaptive plan, which is not
      // columnar, so its batches are converted to rows.
      val shuffled = writtenPlan {
        spark.read.format(format).option("end", "10").option("arrowBatchSize", 2).load()
          .repartition(2)
          .write.format(format).option("table", "shuffled_write").mode("append").save()
      }
      assert(shuffled.isInstanceOf[AdaptiveSparkPlanExec], shuffled)
      assert(find(shuffled)(_.isInstanceOf[ColumnarToRowExec]).isDefined, shuffled)
      assert(TestColumnarSink.rows("shuffled_write").sorted == expected)

      // A query that supports rows too, such as the scan of a cached DataFrame, is written as rows.
      val cached = spark.read.format(format).option("end", "10").load().select("id").cache()
      try {
        cached.count()
        val writes = QueryTest.withQueryExecutionsCaptured(spark) {
          cached.write.format(format).option("table", "cached_write").mode("append").save()
        }.flatMap(_.executedPlan.collectFirst { case writeExec: V2TableWriteExec => writeExec })
        val write = writes.head
        assert(write.supportsColumnarInput, write)
        assert(write.query.isInstanceOf[InMemoryTableScanExec], write.query)
        assert(write.query.supportsColumnar && write.query.supportsRowBased, write.query)
        assert(!write.writesColumnar, write)
        assert(TestColumnarSink.rows("cached_write").sorted == expected.map { case (id, _) =>
          (id, "")
        })
      } finally {
        cached.unpersist()
      }
      assert(TestColumnarSink.rows("row_write") == expected.map { case (id, name) =>
        (id, name.toUpperCase(Locale.ROOT))
      })
    }
  }

  test("copy the Arrow columns of every type, or convert the columns of a batch") {
    val nullable = Seq(
      "id % 2 = 0", "CAST(id AS TINYINT)", "CAST(id AS SMALLINT)", "CAST(id AS INT)", "id",
      "CAST(id AS FLOAT)", "CAST(id AS DOUBLE)", "CAST(id AS DECIMAL(10, 2))",
      "CAST(id AS DECIMAL(38, 10))", "CAST(id AS STRING)", "CAST(CAST(id AS STRING) AS BINARY)",
      "DATE_ADD(DATE'2020-01-01', CAST(id AS INT))", "TIMESTAMP_SECONDS(id)",
      "CAST(TIMESTAMP_SECONDS(id) AS TIMESTAMP_NTZ)", "MAKE_YM_INTERVAL(CAST(id AS INT), 1)",
      "MAKE_DT_INTERVAL(CAST(id AS INT))", "MAKE_INTERVAL(0, 1, 0, CAST(id AS INT))",
      "PARSE_JSON(CAST(id AS STRING))", "ARRAY(id, NULL)", "ARRAY(CAST(id AS STRING))",
      "MAP(CAST(id AS STRING), id)", "MAP(id, ARRAY(CAST(id AS STRING)))",
      "NAMED_STRUCT('a', id, 'b', CAST(id AS STRING))", "ARRAY(NAMED_STRUCT('x', id))")
    // c0 is not nullable, c1 has the null type, and the other columns have nulls.
    val df = spark.range(7).selectExpr(
      Seq("id AS c0", "NULL AS c1") ++ nullable.zipWithIndex.map { case (expr, i) =>
        s"IF(id % 3 = 1, NULL, $expr) AS c${i + 2}"
      }: _*)
    val schema = df.schema
    val rows = df.queryExecution.toRdd.map(_.copy()).collect().toSeq
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("test", 0, Long.MaxValue)
    try {
      // Writes a batch with the session time zone America/Los_Angeles, and returns the sizes of
      // the batches that the data writer got, the time zone of their timestamp column c14, which
      // a copy keeps, the Arrow types of their string column c11, and their rows. The data writer
      // keeps the data of the batches, and reads it once `overwrite` overwrote the batch.
      def writeAndRead(
          batch: ColumnarBatch,
          overwrite: () => Unit,
          schema: StructType = schema,
          maxRecordsPerBatch: Int = 10,
          maxBytesPerBatch: Long = Int.MaxValue)
          : (Seq[Int], Seq[String], Seq[ArrowType], Seq[InternalRow]) = {
        val sizes = ArrayBuffer.empty[Int]
        val timeZones = ArrayBuffer.empty[String]
        val stringTypes = ArrayBuffer.empty[ArrowType]
        val kept = ArrayBuffer.empty[TestArrowExport]
        def field(batch: ColumnarBatch, name: String) = batch.column(schema.fieldIndex(name))
          .asInstanceOf[ArrowColumnVector].getValueVector.getField
        val dataWriter = new DataWriter[ColumnarBatch] {
          override def write(batch: ColumnarBatch): Unit = {
            sizes += batch.numRows()
            timeZones += field(batch, "c14").getType.asInstanceOf[ArrowType.Timestamp].getTimezone
            stringTypes += field(batch, "c11").getType
            kept += TestArrowExport(batch)
          }
          override def commit(): WriterCommitMessage = null
          override def abort(): Unit = {}
          override def close(): Unit = kept.foreach(_.close())
        }
        val writer = new ArrowBatchingDataWriter(dataWriter, ArrowBatchingOptions(
          schema, "America/Los_Angeles", maxRecordsPerBatch, maxBytesPerBatch))
        val result = try {
          writer.writeBatch(batch)
          writer.commit()
          overwrite()
          val toUnsafe = UnsafeProjection.create(schema)
          val written = kept.toSeq.flatMap { exported =>
            exported.read(_.rowIterator().asScala.map(toUnsafe(_).copy()).toSeq)
          }
          kept.clear()
          (sizes.toSeq, timeZones.toSeq, stringTypes.toSeq, written)
        } finally {
          writer.close()
        }
        // Once closed, the writer holds nothing, which a leak would still hold.
        assert(writer.allocator.getAllocatedMemory == 0)
        result
      }

      // Runs `f` with a batch of Arrow vectors with the time zone UTC, and a function that
      // overwrites the vectors with the rows in reverse order.
      def withArrowBatch(largeVarTypes: Boolean)(f: (ColumnarBatch, () => Unit) => Unit): Unit = {
        val root = VectorSchemaRoot.create(
          ArrowUtils.toArrowSchema(
            schema, "UTC", errorOnDuplicatedFieldNames = true, largeVarTypes = largeVarTypes),
          allocator)
        try {
          val arrowWriter = ArrowWriter.create(root)
          rows.foreach(arrowWriter.write)
          arrowWriter.finish()
          val batch = new ColumnarBatch(
            root.getFieldVectors.asScala.map(new ArrowColumnVector(_): ColumnVector).toArray,
            rows.length)
          f(batch, () => {
            arrowWriter.reset()
            rows.reverse.foreach(arrowWriter.write)
            arrowWriter.finish()
          })
        } finally {
          root.close()
        }
      }

      val expected = rows.map(UnsafeProjection.create(schema)(_).copy())
      val utf8 = ArrowType.Utf8.INSTANCE
      Seq(false, true).foreach { largeVarTypes =>
        withArrowBatch(largeVarTypes) { (batch, _) =>
          // The size of a range of rows, which the batches of a copy end at, is the size of a slice
          // of these rows.
          (0 until batch.numCols()).foreach { i =>
            val vector = batch.column(i).asInstanceOf[ArrowColumnVector].getValueVector
            Seq((0, 7), (1, 4), (3, 7), (6, 7), (2, 2)).foreach { case (start, end) =>
              val pair = vector.getTransferPair(vector.getAllocator)
              pair.splitAndTransfer(start, end - start)
              val sliceSize =
                try pair.getTo.getBufferSizeFor(end - start) finally pair.getTo.close()
              assert(ArrowBatchingDataWriter.rangeSize(vector, start, end) == sliceSize,
                s"${vector.getField} [$start, $end)")
            }
          }
        }
      }
      withArrowBatch(largeVarTypes = false) { (batch, overwrite) =>
        // The columns are Arrow vectors with the types of the schema, so they are copied.
        assert(writeAndRead(batch, overwrite) == ((Seq(7), Seq("UTC"), Seq(utf8), expected)))
      }
      withArrowBatch(largeVarTypes = false) { (batch, overwrite) =>
        // In slices of at most 3 rows, which start in the middle of the bytes of validity.
        assert(writeAndRead(batch, overwrite, maxRecordsPerBatch = 3) ==
          ((Seq(3, 3, 1), Seq.fill(3)("UTC"), Seq.fill(3)(utf8), expected)))
      }
      withArrowBatch(largeVarTypes = false) { (batch, overwrite) =>
        // A copied batch also ends after the first row with which it holds the maximum size.
        assert(writeAndRead(batch, overwrite, maxBytesPerBatch = 1) ==
          ((Seq.fill(7)(1), Seq.fill(7)("UTC"), Seq.fill(7)(utf8), expected)))
      }
      withArrowBatch(largeVarTypes = true) { (batch, overwrite) =>
        // Each column of another Arrow type than that of the schema, here a string or a binary, is
        // converted on its own, and the other columns are copied.
        assert(writeAndRead(batch, overwrite, maxRecordsPerBatch = 3) ==
          ((Seq(3, 3, 1), Seq.fill(3)("UTC"), Seq.fill(3)(utf8), expected)))
      }
      withArrowBatch(largeVarTypes = true) { (batch, overwrite) =>
        // The size of the converted columns counts too.
        assert(writeAndRead(batch, overwrite, maxBytesPerBatch = 1) ==
          ((Seq.fill(7)(1), Seq.fill(7)("UTC"), Seq.fill(7)(utf8), expected)))
      }

      // On-heap vectors are converted too. They do not support the null type.
      val onHeapSchema = StructType(schema.filterNot(_.name == "c1"))
      val onHeapRows = rows.map(row => InternalRow.fromSeq(row.toSeq(schema).patch(1, Nil, 1)))
      val vectors: Seq[WritableColumnVector] =
        OnHeapColumnVector.allocateColumns(onHeapRows.length, onHeapSchema).toSeq
      try {
        val converter = new RowToColumnConverter(onHeapSchema)
        onHeapRows.foreach(converter.convert(_, vectors.toArray))
        val batch = new ColumnarBatch(vectors.map(v => v: ColumnVector).toArray, onHeapRows.length)
        assert(writeAndRead(batch, () => (), onHeapSchema, maxRecordsPerBatch = 3) ==
          ((Seq(3, 3, 1), Seq.fill(3)("America/Los_Angeles"), Seq.fill(3)(utf8),
            onHeapRows.map(UnsafeProjection.create(onHeapSchema)(_).copy()))))
      } finally {
        vectors.foreach(_.close())
      }
    } finally {
      allocator.close()
    }
  }

  test("write the pending rows of converted batches before a copied batch") {
    val schema = StructType.fromDDL("id BIGINT, name STRING")
    val writer = new ArrowBatchingDataWriter(
      new TestDataWriter(fail = false), ArrowBatchingOptions(schema, "UTC", 10, Int.MaxValue))
    val onHeap = new TestBatchReader(0, 3, schema)
    val arrow = new TestArrowBatchReader(3, 7, schema, 4)
    try {
      // The rows of the on-heap batch stay in the batch of rows until the Arrow batch is copied.
      assert(onHeap.next())
      writer.writeBatch(onHeap.get())
      assert(arrow.next())
      writer.writeBatch(arrow.get())
      val message = writer.commit().asInstanceOf[TestCommitMessage]
      assert(message.rows == (0L until 7L).map(id => (id, s"name$id")))
      assert(message.batchSizes == Seq(3, 4))
    } finally {
      writer.close()
      onHeap.close()
      arrow.close()
    }
    assert(writer.allocator.getAllocatedMemory == 0)
  }

  test("a copied batch ends at the byte limit as a batch of rows does") {
    val schema = StructType.fromDDL("id BIGINT, name STRING")
    def write(batch: ColumnarBatch, maxBytesPerBatch: Long): TestCommitMessage = {
      val writer = new ArrowBatchingDataWriter(new TestDataWriter(fail = false),
        ArrowBatchingOptions(schema, "UTC", 5, maxBytesPerBatch))
      val message = try {
        writer.writeBatch(batch)
        writer.commit().asInstanceOf[TestCommitMessage]
      } finally {
        writer.close()
      }
      assert(writer.allocator.getAllocatedMemory == 0)
      message
    }
    val onHeap = new TestBatchReader(0, 20, schema)
    val arrow = new TestArrowBatchReader(0, 20, schema, 20)
    // The names are large strings, which are converted, while the ids are copied.
    val largeNames = new TestArrowBatchReader(0, 20, schema, 20, largeVarTypes = true)
    try {
      assert(onHeap.next() && arrow.next() && largeNames.next())
      val vectors = (0 until 2).map {
        arrow.get().column(_).asInstanceOf[ArrowColumnVector].getValueVector
      }
      // The size of the first 2 rows. The names of the ids from 10 are 1 byte longer.
      val twoRows = vectors.map(ArrowBatchingDataWriter.rangeSize(_, 0, 2)).sum
      Seq(twoRows - 1, twoRows, twoRows + 1, 3 * twoRows).foreach { limit =>
        val rows = write(onHeap.get(), limit)
        assert(rows.batchSizes.sum == 20)
        // Below 3 times the size of 2 rows, a window of 5 rows ends with a batch that does not hold
        // the limit, so the next window starts at that batch, and converts its names again.
        Seq(arrow, largeNames).foreach { reader =>
          assert(write(reader.get(), limit) == rows, s"limit $limit")
        }
      }
      // The limit ends the batches every 2 rows, also across the windows of 5 rows.
      assert(write(arrow.get(), twoRows).batchSizes == Seq.fill(10)(2))
    } finally {
      onHeap.close()
      arrow.close()
      largeNames.close()
    }
  }

  test("a window does not convert the rows of a batch that ends at the byte limit with it") {
    val schema = StructType.fromDDL("id BIGINT, name STRING")
    val arrow = new TestArrowBatchReader(0, 10, schema, 10)
    val largeNames = new TestArrowBatchReader(0, 10, schema, 10, largeVarTypes = true)
    try {
      assert(arrow.next() && largeNames.next())
      def vectors(reader: TestArrowBatchReader): Seq[ValueVector] = (0 until 2).map {
        reader.get().column(_).asInstanceOf[ArrowColumnVector].getValueVector
      }
      // The size of 3 rows, which have the same size, since the names have the same length.
      val threeRows = vectors(arrow).map(ArrowBatchingDataWriter.rangeSize(_, 0, 3)).sum
      // The ids are copied, and the large strings of the names are converted, one by one.
      var conversions = 0
      val Seq(ids, names) = vectors(largeNames)
      val batch = new ColumnarBatch(Array[ColumnVector](
        new ArrowColumnVector(ids),
        new ArrowColumnVector(names) {
          override def getUTF8String(rowId: Int): UTF8String = {
            conversions += 1
            super.getUTF8String(rowId)
          }
        }), 10)
      val writer = new ArrowBatchingDataWriter(new TestDataWriter(fail = false),
        ArrowBatchingOptions(schema, "UTC", 6, threeRows))
      val message = try {
        writer.writeBatch(batch)
        writer.commit().asInstanceOf[TestCommitMessage]
      } finally {
        writer.close()
      }
      assert(writer.allocator.getAllocatedMemory == 0)
      // The second batch holds the limit at the end of the window of 6 rows, as a batch of rows
      // would, so the next window starts after it, and each row is converted once.
      assert(message.batchSizes == Seq(3, 3, 3, 1))
      assert(message.rows == (0L until 10L).map(id => (id, s"name$id")))
      assert(conversions == 10)
    } finally {
      arrow.close()
      largeNames.close()
    }
  }

  test("an empty columnar batch does not end the pending batch of rows") {
    val schema = StructType.fromDDL("id BIGINT, name STRING")
    val writer = new ArrowBatchingDataWriter(
      new TestDataWriter(fail = false), ArrowBatchingOptions(schema, "UTC", 10, Int.MaxValue))
    val first = new TestBatchReader(0, 2, schema)
    val empty = new TestArrowBatchReader(0, 0, schema, 1)
    val second = new TestBatchReader(2, 4, schema)
    try {
      assert(first.next() && second.next())
      writer.writeBatch(first.get())
      writer.writeBatch(empty.get())
      writer.writeBatch(second.get())
      assert(writer.commit().asInstanceOf[TestCommitMessage].batchSizes == Seq(4))
    } finally {
      writer.close()
      first.close()
      empty.close()
      second.close()
    }
    assert(writer.allocator.getAllocatedMemory == 0)
  }

  test("release the converted columns when a later column fails to convert") {
    val schema = StructType.fromDDL("id BIGINT, a DECIMAL(10, 2), b DECIMAL(10, 2)")
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("test", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(new Schema(Seq(
      Field.notNullable("id", new ArrowType.Int(64, true)),
      Field.nullable("a", new ArrowType.Decimal(38, 2, 128)),
      Field.nullable("b", new ArrowType.Decimal(38, 2, 128))).asJava), allocator)
    try {
      root.allocateNew()
      root.getVector("id").asInstanceOf[BigIntVector].setSafe(0, 0)
      // A decimal of another precision than that of the schema is converted, and the value of b
      // does not fit the precision of the schema.
      root.getVector("a").asInstanceOf[DecimalVector].setSafe(0, new java.math.BigDecimal("1.00"))
      root.getVector("b").asInstanceOf[DecimalVector]
        .setSafe(0, new java.math.BigDecimal("1000000000000.00"))
      root.setRowCount(1)
      val batch = new ColumnarBatch(
        root.getFieldVectors.asScala.map(new ArrowColumnVector(_): ColumnVector).toArray, 1)
      val writer = new ArrowBatchingDataWriter(
        new TestDataWriter(fail = false), ArrowBatchingOptions(schema, "UTC", 10, Int.MaxValue))
      try {
        val e = intercept[SparkArithmeticException](writer.writeBatch(batch))
        assert(e.getCondition == "NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION")
        // The vector of a, which was converted, is released with the failure.
        assert(writer.allocator.getAllocatedMemory == 0)
      } finally {
        writer.close()
      }
    } finally {
      root.close()
      allocator.close()
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

  test("a streaming scan reads only the columns that the query needs") {
    val dataSource = new TestRangeDataSource(options("end" -> "3"))
    val builder = new ColumnarScanBuilder(format, dataSource.schema(), () => dataSource)
    builder.pruneColumns(StructType.fromDDL("name STRING"))
    val scan = builder.build()
    val stream = scan.toMicroBatchStream(Utils.createTempDir().getPath)
    assert(scan.readSchema() == StructType.fromDDL("name STRING"))
    // The stream reader gets the pruned schema, and its batches only have these columns.
    val start = stream.initialOffset()
    val end = stream.asInstanceOf[SupportsAdmissionControl]
      .latestOffset(start, ReadLimit.allAvailable())
    val factory = stream.createReaderFactory()
    val names = stream.planInputPartitions(start, end).toSeq.flatMap { partition =>
      val reader = factory.createColumnarReader(partition)
      try {
        assert(reader.next())
        val batch = reader.get()
        assert(batch.numCols() == 1)
        (0 until batch.numRows()).map(batch.column(0).getUTF8String(_).toString)
      } finally {
        reader.close()
      }
    }
    assert(names == Seq("name0", "name1", "name2"))
    // A streaming scan has no batch planner, which a data source may not support.
    assert(dataSource.batchPlanners.get() == 0)
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
      // A micro-batch writes the batches of the scan, without a transition to rows.
      val plan =
        query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
      assert(plan.collectFirst { case write: V2TableWriteExec => write.query }
        .exists(_.isInstanceOf[MicroBatchScanExec]), plan)
    } finally {
      query.stop()
    }
    assert(TestColumnarSink.rows(table).sorted == (0L until 5L).map(id => (id, s"name$id")))
    assert(TestColumnarSink.epochs.get(table) == Seq(0L))
  }

  test("the stream reader gets its checkpoint location and the start of each micro-batch") {
    val table = "stream_reader_calls"
    // The calls of an earlier run of the test in the same JVM.
    TestRangeStreamReader.checkpointLocations.remove(table)
    TestRangeStreamReader.starts.remove(table)
    TestColumnarSink.committed.remove(s"${table}_sink")
    val checkpoint = Utils.createTempDir()
    val query = spark.readStream.format(format).option("end", "5").option("table", table).load()
      .writeStream
      .format(format)
      .option("table", s"${table}_sink")
      .option("checkpointLocation", checkpoint.getPath)
      .start()
    try {
      query.processAllAvailable()
      // The progress of the query reports the latest offset that the reader returned.
      assert(query.lastProgress.sources.head.latestOffset == """{"offset":5}""")
    } finally {
      query.stop()
    }
    assert(TestColumnarSink.rows(s"${table}_sink").length == 5)
    val checkpointLocation = TestRangeStreamReader.checkpointLocations.get(table)
    assert(checkpointLocation.endsWith(s"${checkpoint.getCanonicalPath}/sources/0"),
      checkpointLocation)
    // The first micro-batch starts after the initial offset, and the next ones after the end of
    // the previous one.
    val starts = TestRangeStreamReader.starts.get(table)
    assert(starts.head == """{"offset":0}""")
    assert(starts.tail.nonEmpty && starts.tail.forall(_ == """{"offset":5}"""), starts)
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
  // The number of batch planners that this data source created, and of the batch scans that they
  // planned.
  val batchPlanners = new AtomicInteger()
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

  // With the option "arrowBatchSize", the batch scans return Arrow batches of that size.
  override def batchPlanner(schema: StructType): ColumnarBatchPlanner = {
    batchPlanners.incrementAndGet()
    new TestRangePlanner(end, schema, options.getBoolean("failPlanning", false), plans,
      options.getInt("arrowBatchSize", 0))
  }

  override def streamReader(
      schema: StructType,
      checkpointLocation: String): ColumnarStreamReader = {
    TestRangeStreamReader.checkpointLocations.put(table, checkpointLocation)
    new TestRangeStreamReader(end, schema, table)
  }

  // With the option "keep", the data writers keep the data of the batches until they commit.
  override def writer(schema: StructType, overwrite: Boolean): ColumnarWriter = {
    TestColumnarSink.writers.add(table)
    new TestSinkWriter(
      table, overwrite, options.getBoolean("fail", false), options.getBoolean("keep", false))
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

class TestRangePlanner(
    end: Long,
    schema: StructType,
    fail: Boolean,
    plans: AtomicInteger,
    arrowBatchSize: Int)
  extends ColumnarBatchPlanner {
  private var limit = Long.MaxValue
  private var readSchema = schema

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
    val hi = math.min(end, limit)
    val mid = hi / 2
    ColumnarPlannedBatch(
      Array(TestRangePartition(0, mid), TestRangePartition(mid, hi)),
      new TestRangeReaderFactory(readSchema, arrowBatchSize))
  }
}

/** @param arrowBatchSize the size of the Arrow batches to return, or 0 for on-heap batches */
class TestRangeReaderFactory(readSchema: StructType, arrowBatchSize: Int)
  extends ColumnarOnlyPartitionReaderFactory {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val range = partition.asInstanceOf[TestRangePartition]
    if (arrowBatchSize > 0) {
      new TestArrowBatchReader(range.lo, range.hi, readSchema, arrowBatchSize)
    } else {
      new TestBatchReader(range.lo, range.hi, readSchema)
    }
  }
}

/** @param table the option "table" of the read, under which it records its calls */
class TestRangeStreamReader(end: Long, schema: StructType, table: String)
  extends ColumnarStreamReader {
  private def offset(json: String): Long = json.stripPrefix("{\"offset\":").stripSuffix("}").toLong

  override def initialOffset(): String = "{\"offset\":0}"

  override def latestOffset(start: String): String = {
    TestRangeStreamReader.starts.compute(table, (_, old) => Option(old).getOrElse(Nil) :+ start)
    s"""{"offset":$end}"""
  }

  override def partitions(start: String, end: String): Array[InputPartition] = {
    Array(TestRangePartition(offset(start), offset(end)))
  }

  override def read(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val range = partition.asInstanceOf[TestRangePartition]
    new TestBatchReader(range.lo, range.hi, schema)
  }
}

object TestRangeStreamReader {
  // By the option "table" of the read: the checkpoint location of its stream reader, and the
  // start offsets that the stream reader got for the latest offset.
  val checkpointLocations = new ConcurrentHashMap[String, String]()
  val starts = new ConcurrentHashMap[String, Seq[String]]()
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

/**
 * Returns the ids [lo, hi) and their names in Arrow batches of at most `batchSize` rows. It writes
 * each batch to the same Arrow vectors, over the previous one.
 */
/** @param largeVarTypes whether the strings are `large_utf8`, which the writers convert */
class TestArrowBatchReader(
    lo: Long,
    hi: Long,
    schema: StructType,
    batchSize: Int,
    largeVarTypes: Boolean = false)
  extends PartitionReader[ColumnarBatch] {
  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator("test arrow batch reader", 0, Long.MaxValue)
  private val root = VectorSchemaRoot.create(
    ArrowUtils.toArrowSchema(
      schema, "UTC", errorOnDuplicatedFieldNames = true, largeVarTypes = largeVarTypes),
    allocator)
  private val arrowWriter = ArrowWriter.create(root)
  // The vectors are reused, so a consumer does not release them.
  private val batch = new ColumnarBatch(
    root.getFieldVectors.asScala.map(new ReusedArrowColumnVector(_): ColumnVector).toArray)
  private var start = lo

  override def next(): Boolean = {
    if (start >= hi) {
      false
    } else {
      val end = math.min(hi, start + batchSize)
      arrowWriter.reset()
      (start until end).foreach { id =>
        arrowWriter.write(InternalRow.fromSeq(schema.fieldNames.toSeq.map {
          case "id" => id
          case "name" => UTF8String.fromString(s"name$id")
        }))
      }
      arrowWriter.finish()
      batch.setNumRows((end - start).toInt)
      start = end
      true
    }
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    root.close()
    allocator.close()
  }
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

/**
 * @param keep whether to keep the data of each batch until the commit, as a native data writer
 *             can, by taking ownership of the Arrow buffers of the batch
 */
class TestDataWriter(fail: Boolean, keep: Boolean = false) extends DataWriter[ColumnarBatch] {
  private val rows = ArrayBuffer.empty[(Long, String)]
  private val batchSizes = ArrayBuffer.empty[Int]
  private val kept = ArrayBuffer.empty[TestArrowExport]

  override def write(batch: ColumnarBatch): Unit = {
    if (fail) {
      throw new IllegalStateException("injected failure")
    }
    assert((0 until batch.numCols()).forall(batch.column(_).isInstanceOf[ArrowColumnVector]))
    batchSizes += batch.numRows()
    if (keep) {
      kept += TestArrowExport(batch)
    } else {
      read(batch)
    }
  }

  private def read(batch: ColumnarBatch): Unit = {
    (0 until batch.numRows()).foreach { i =>
      // A batch without the column of the names, such as that of a cached DataFrame of the ids,
      // gets empty names.
      val name = if (batch.numCols() > 1) batch.column(1).getUTF8String(i).toString else ""
      rows += ((batch.column(0).getLong(i), name))
    }
  }

  override def commit(): WriterCommitMessage = {
    kept.foreach(_.read(read))
    kept.clear()
    TestCommitMessage(rows.toSeq, batchSizes.toSeq)
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    kept.foreach(_.close())
    kept.clear()
  }
}

/** A batch exported to the Arrow C data interface, which keeps its data until it is read. */
class TestArrowExport(array: ArrowArray, schema: ArrowSchema) {
  private var arrayClosed = false
  private var schemaClosed = false

  /**
   * Imports the batch, and passes it to `f`. Each import closes the struct that it gets, also when
   * it fails, so `close` then only releases the struct that is still open.
   */
  def read[T](f: ColumnarBatch => T): T = {
    val arrowSchema = try Data.importSchema(ArrowUtils.rootAllocator, schema, null) finally {
      schemaClosed = true
    }
    val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
    try {
      try Data.importIntoVectorSchemaRoot(ArrowUtils.rootAllocator, array, root, null) finally {
        arrayClosed = true
      }
      f(new ColumnarBatch(
        root.getFieldVectors.asScala.map(new ArrowColumnVector(_): ColumnVector).toArray,
        root.getRowCount))
    } finally {
      root.close()
    }
  }

  def close(): Unit = {
    if (!arrayClosed) {
      arrayClosed = true
      array.release()
      array.close()
    }
    if (!schemaClosed) {
      schemaClosed = true
      schema.release()
      schema.close()
    }
  }
}

object TestArrowExport {
  def apply(batch: ColumnarBatch): TestArrowExport = {
    val vectors = (0 until batch.numCols()).map { i =>
      batch.column(i).asInstanceOf[ArrowColumnVector].getValueVector.asInstanceOf[FieldVector]
    }
    val root =
      new VectorSchemaRoot(vectors.map(_.getField).asJava, vectors.asJava, batch.numRows())
    val array = ArrowArray.allocateNew(ArrowUtils.rootAllocator)
    val schema = ArrowSchema.allocateNew(ArrowUtils.rootAllocator)
    Data.exportVectorSchemaRoot(ArrowUtils.rootAllocator, root, null, array, schema)
    new TestArrowExport(array, schema)
  }
}

class TestSinkWriter(table: String, overwrite: Boolean, fail: Boolean, keep: Boolean)
  extends ColumnarWriter {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    new TestDataWriter(fail, keep)
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

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

package org.apache.spark.sql.streaming

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow, StructFilters}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Predicate => CatalystPredicate}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.{Dataset, ExpressionUtils, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, PartitionOffset, SupportsRealTimeRead}
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead.RecordStatus
import org.apache.spark.sql.execution.datasources.v2.{RealTimeStreamScanExec, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Tests the pushdowns to the scans of the streaming sources of real-time queries. */
class StreamingV2ScanPushDownRealTimeSuite extends StreamRealTimeModeSuiteBase {

  import testImplicits._

  // The pruning of the scans of streaming queries is disabled by default.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED, true)

  test("read only the columns that a real-time query needs") {
    val input = new PruningLowLatencyStream(spark)
    val df = readTable(new PruningRealTimeTable(input)).where("extra > 10").select("data")
    testStream(df, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      AddData(input, (1L, "a", 10), (2L, "b", 20), (3L, "c", 30)),
      CheckAnswer("b", "c"),
      AssertOnQuery { query =>
        // The scan only reads the columns of the query, and the scan node of the real-time
        // query outputs them, as the readers return them.
        val readSchemas = query.logicalPlan.collect {
          case relation: StreamingDataSourceV2ScanRelation => relation.scan.readSchema()
        }
        assert(readSchemas.map(_.fieldNames.toSeq) === Seq(Seq("_2", "_3")))
        val scanOutputs = query.lastExecution.executedPlan.collect {
          case scan: RealTimeStreamScanExec => scan.output.map(_.name)
        }
        assert(scanOutputs === Seq(Seq("_2", "_3")))
        true
      },
      AddData(input, (4L, "d", 5), (5L, "e", 50)),
      CheckAnswer("b", "c", "e"))
  }

  test("push down the filters of a real-time query") {
    withSQLConf(SQLConf.STREAMING_V2_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      val input = new PruningLowLatencyStream(spark)
      val table = new PruningRealTimeTable(input, takesFilters = true)
      PruningLowLatencyStream.dropped.set(0)
      val df = readTable(table).where("extra > 10").select("data")
      // The scan builder gets the filter, and its stream drops the rows that fail it, while each
      // batch applies it after the scan too, as its check finds.
      testStream(df, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        StartStream(),
        AddData(input, (1L, "a", 10), (2L, "b", 20), (3L, "c", 30)),
        CheckAnswer("b", "c"),
        AssertOnQuery { query =>
          assert(table.pushed === Seq("GreaterThan(_3,10)"))
          val pushed = query.logicalPlan.collect {
            case relation: StreamingDataSourceV2ScanRelation => relation.pushedFilters
          }
          assert(pushed.map(_.map(_.references.map(_.name).toSet)) === Seq(Seq(Set("_3"))))
          true
        },
        AddData(input, (4L, "d", 5), (5L, "e", 50)),
        CheckAnswer("b", "c", "e"),
        AssertOnQuery(_ => PruningLowLatencyStream.dropped.get() == 2))
    }
  }

  test("fail a real-time batch whose query no longer applies the filters that its scan got") {
    withSQLConf(SQLConf.STREAMING_V2_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      // The filter has a value that changes each time that it is evaluated, which the optimization
      // at the start folds, as it would fold a function of the time that it does not replace.
      ChangingValue.values.set(0)
      val input = new PruningLowLatencyStream(spark)
      val table = new PruningRealTimeTable(input, takesFilters = true)
      val df = readTable(table)
        .where(col("extra") < ExpressionUtils.column(ChangingValue())).select("data")
      // The stream drops the rows that fail extra < 1, while the plan of the batch has a larger
      // value, so it would keep some of them: the batch fails before it reads them.
      testStream(df, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        StartStream(),
        ExpectFailure[SparkException] { e =>
          assert(e.asInstanceOf[SparkException].getCondition === "STREAMING_SCAN_FILTERS_CHANGED")
        })
      assert(table.pushed === Seq("LessThan(_3,1)"))
    }
  }

  // The real-time query of the columns (id, data, extra) of `table`.
  private def readTable(table: PruningRealTimeTable): DataFrame =
    Dataset.ofRows(spark, StreamingRelationV2(None, "pruning_real_time", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra")
}

/**
 * A low-latency memory stream of (id, data, extra), whose readers return the columns that its
 * table was asked for, see [[PruningRealTimeTable]].
 */
private class PruningLowLatencyStream(sparkSession: SparkSession)
  extends LowLatencyMemoryStream[(Long, String, Int)](
    PruningLowLatencyStream.ids.getAndIncrement(), sparkSession, numPartitions = 1)(
    PruningLowLatencyStream.encoder) {

  // The ordinals of the columns that the readers return, and their schema.
  @volatile var columns: Option[(Array[Int], StructType)] = None
  // The readers drop the rows that fail these filters, bound to all the columns of the stream.
  @volatile var filters: Seq[Expression] = Nil

  override def createReaderFactory(): PartitionReaderFactory = columns match {
    case Some((ordinals, schema)) =>
      new PruningRealTimeReaderFactory(super.createReaderFactory(), ordinals, schema, filters)
    case None => super.createReaderFactory()
  }
}

private object PruningLowLatencyStream {
  // Far from the ids of the other low-latency memory streams of the JVM, which name the endpoints.
  val ids = new AtomicInteger(1 << 20)
  val encoder: Encoder[(Long, String, Int)] =
    Encoders.tuple(Encoders.scalaLong, Encoders.STRING, Encoders.scalaInt)
  // The number of rows that the readers dropped, as they failed the filters.
  val dropped = new AtomicInteger()
}

/**
 * A table over a [[PruningLowLatencyStream]], whose scans only read the columns they are asked.
 * With `takesFilters`, it declares STREAMING_FILTER_PUSHDOWN, and its scan builder takes all the
 * filters, which it records, and its streams drop the rows that fail them.
 */
private class PruningRealTimeTable(input: PruningLowLatencyStream, takesFilters: Boolean = false)
  extends Table with SupportsRead {
  @volatile var pushed: Seq[String] = Nil

  override def name(): String = "pruning_real_time"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = util.EnumSet.of(
      TableCapability.MICRO_BATCH_READ, TableCapability.STREAMING_COLUMN_PRUNING)
    if (takesFilters) capabilities.add(TableCapability.STREAMING_FILTER_PUSHDOWN)
    capabilities
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
      private var columns = schema()
      private var filters = Array.empty[Filter]

      override def pushFilters(filters: Array[Filter]): Array[Filter] = {
        if (takesFilters) {
          this.filters = filters
          pushed = filters.map(_.toString).toSeq
          filters
        } else {
          filters
        }
      }

      override def pushedFilters(): Array[Filter] = filters

      override def pruneColumns(requiredSchema: StructType): Unit = {
        columns = StructType(requiredSchema.fieldNames.map(schema()(_)))
      }

      override def build(): Scan = new Scan {
        override def readSchema(): StructType = columns

        override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
          input.columns = Some(columns.fieldNames.map(schema().fieldIndex) -> columns)
          input.filters = filters.toSeq.flatMap(StructFilters.filterToExpression(_, name =>
            schema().getFieldIndex(name).map(i => BoundReference(i, schema()(i).dataType, true))))
          input
        }
      }
    }
  }
}

/**
 * The readers of a [[PruningLowLatencyStream]], which return the columns of `ordinals`, with the
 * schema `columns`, of the rows that pass `filters`, and drop the others.
 */
private class PruningRealTimeReaderFactory(
    input: PartitionReaderFactory,
    ordinals: Array[Int],
    columns: StructType,
    filters: Seq[Expression]) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val reader = input.createReader(partition).asInstanceOf[SupportsRealTimeRead[InternalRow]]
    new SupportsRealTimeRead[InternalRow] {
      private val projection = ProjectingInternalRow(columns, ordinals.toSeq)
      private val predicates = filters.map(CatalystPredicate.createInterpreted)

      // Whether the row of the reader passes the filters, or else counts it as dropped.
      private def keeps(): Boolean = predicates.forall(_.eval(reader.get())) || {
        PruningLowLatencyStream.dropped.incrementAndGet()
        false
      }

      override def nextWithTimeout(
          startTimeMs: java.lang.Long,
          timeoutMs: java.lang.Long): RecordStatus = {
        var status = reader.nextWithTimeout(startTimeMs, timeoutMs)
        while (status.hasRecord && !keeps()) {
          status = reader.nextWithTimeout(startTimeMs, timeoutMs)
        }
        status
      }

      override def next(): Boolean = {
        while (reader.next()) {
          if (keeps()) return true
        }
        false
      }

      override def get(): InternalRow = {
        projection.project(reader.get())
        projection
      }

      override def getOffset: PartitionOffset = reader.getOffset

      override def close(): Unit = reader.close()
    }
  }
}

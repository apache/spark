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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, PartitionOffset, SupportsRealTimeRead}
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead.RecordStatus
import org.apache.spark.sql.execution.datasources.v2.{RealTimeStreamScanExec, StreamingDataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Tests the column pruning of the scans of the streaming sources of real-time queries. */
class StreamingV2ScanPushDownRealTimeSuite extends StreamRealTimeModeSuiteBase {

  import testImplicits._

  // The pruning of the scans of streaming queries is disabled by default.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED, true)

  test("read only the columns that a real-time query needs") {
    val input = new PruningLowLatencyStream(spark)
    val table = new PruningRealTimeTable(input)
    val df = Dataset.ofRows(spark, StreamingRelationV2(None, "pruning_real_time", table,
      CaseInsensitiveStringMap.empty(), DataTypeUtils.toAttributes(table.schema()), None, None,
      None)).toDF("id", "data", "extra").where("extra > 10").select("data")
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

  override def createReaderFactory(): PartitionReaderFactory = columns match {
    case Some((ordinals, schema)) =>
      new PruningRealTimeReaderFactory(super.createReaderFactory(), ordinals, schema)
    case None => super.createReaderFactory()
  }
}

private object PruningLowLatencyStream {
  // Far from the ids of the other low-latency memory streams of the JVM, which name the endpoints.
  val ids = new AtomicInteger(1 << 20)
  val encoder: Encoder[(Long, String, Int)] =
    Encoders.tuple(Encoders.scalaLong, Encoders.STRING, Encoders.scalaInt)
}

/** A table over a [[PruningLowLatencyStream]], whose scans only read the columns they are asked. */
private class PruningRealTimeTable(input: PruningLowLatencyStream)
  extends Table with SupportsRead {

  override def name(): String = "pruning_real_time"

  override def schema(): StructType = input.fullSchema()

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.STREAMING_COLUMN_PRUNING)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ScanBuilder with SupportsPushDownRequiredColumns {
      private var columns = schema()

      override def pruneColumns(requiredSchema: StructType): Unit = {
        columns = StructType(requiredSchema.fieldNames.map(schema()(_)))
      }

      override def build(): Scan = new Scan {
        override def readSchema(): StructType = columns

        override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
          input.columns = Some(columns.fieldNames.map(schema().fieldIndex) -> columns)
          input
        }
      }
    }
  }
}

private class PruningRealTimeReaderFactory(
    input: PartitionReaderFactory,
    ordinals: Array[Int],
    columns: StructType) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val reader = input.createReader(partition).asInstanceOf[SupportsRealTimeRead[InternalRow]]
    new SupportsRealTimeRead[InternalRow] {
      private val projection = ProjectingInternalRow(columns, ordinals.toSeq)

      override def nextWithTimeout(
          startTimeMs: java.lang.Long,
          timeoutMs: java.lang.Long): RecordStatus = reader.nextWithTimeout(startTimeMs, timeoutMs)

      override def next(): Boolean = reader.next()

      override def get(): InternalRow = {
        projection.project(reader.get())
        projection
      }

      override def getOffset: PartitionOffset = reader.getOffset

      override def close(): Unit = reader.close()
    }
  }
}

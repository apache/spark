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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.streaming.ReportsSinkMetrics
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.PackedRowWriterFactory
import org.apache.spark.sql.internal.connector.{SimpleTableProvider, SupportsStreamingUpdateAsAppend}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ReportSinkMetricsSuite extends StreamTest {

  import testImplicits._

  test("test ReportSinkMetrics") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
    var query: StreamingQuery = null

    var metricsMap: java.util.Map[String, String] = null

    val listener = new StreamingQueryListener {

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        metricsMap = event.progress.sink.metrics
      }

      override def onQueryTerminated(
        event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    }

    spark.streams.addListener(listener)

    withTempDir { dir =>
      try {
        query =
          df.writeStream
            .outputMode("append")
            .format("org.apache.spark.sql.streaming.TestSinkProvider")
            .option("checkPointLocation", dir.toString)
            .start()

        inputData.addData(1, 2, 3)

        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        spark.sparkContext.listenerBus.waitUntilEmpty()

        assertResult(metricsMap) {
          Map("metrics-1" -> "value-1", "metrics-2" -> "value-2").asJava
        }
      } finally {
        if (query != null) {
          query.stop()
        }

        spark.streams.removeListener(listener)
      }
    }
  }
}

  case class TestSinkRelation(override val sqlContext: SQLContext, data: DataFrame)
    extends BaseRelation {
    override def schema: StructType = data.schema
  }

  class TestSinkProvider extends SimpleTableProvider
    with DataSourceRegister
    with CreatableRelationProvider with Logging {

    override def getTable(options: CaseInsensitiveStringMap): Table = {
      TestSinkTable
    }

    def createRelation(
        sqlContext: SQLContext,
        mode: SaveMode,
        parameters: Map[String, String],
        data: DataFrame): BaseRelation = {

      TestSinkRelation(sqlContext, data)
    }

    def shortName(): String = "test"
  }

  object TestSinkTable extends Table with SupportsWrite with ReportsSinkMetrics with Logging {

    override def name(): String = "test"

    override def schema(): StructType = StructType(Nil)

    override def capabilities(): java.util.Set[TableCapability] = {
      java.util.EnumSet.of(TableCapability.STREAMING_WRITE)
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      new WriteBuilder with SupportsTruncate with SupportsStreamingUpdateAsAppend {

        override def truncate(): WriteBuilder = this

        override def build(): Write = {
          new Write {
            override def toStreaming: StreamingWrite = {
              new TestSinkWrite()
            }
          }
        }
      }
    }

    override def metrics(): java.util.Map[String, String] = {
      Map("metrics-1" -> "value-1", "metrics-2" -> "value-2").asJava
    }
  }

  class TestSinkWrite()
    extends StreamingWrite with Logging with Serializable {

    def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory =
      PackedRowWriterFactory

    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

    def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

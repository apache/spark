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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.connector.catalog.{InMemoryTableCatalog, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{streaming, InputPartition, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownFilters}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2ScanRelation
import org.apache.spark.sql.execution.streaming.runtime.LongOffset
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

class PushDownPredicateInMicroBatchExecutionSuite extends StreamTest with SharedSparkSession {

  test("Filter pushdown should propagate to MicroBatchStream in DSv2") {
    val checkpointLoc = Utils.createTempDir(namePrefix = "streaming-checkpoint").getCanonicalPath
    val providerClass = classOf[FakeStreamingProvider].getName

    spark.conf.set(s"spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.sql(s"""
      CREATE TABLE testcat.default.streaming_test_table (id BIGINT, part STRING)
      USING $providerClass
      PARTITIONED BY (part)
    """)

    val df = spark.readStream
      .format(s"$providerClass")
      .load()
      .filter("part = 'active'")

    testStream(df)(
      StartStream(checkpointLocation = checkpointLoc),
      // Force a trigger and wait for it to complete
      Execute { q => q.processAllAvailable() },
      Execute { query =>
        // If lastExecution is still null here, the engine hasn't reached the
        // planning phase of the first batch yet.
        assert(query.lastExecution != null,
          "Execution should be initialized after processAllAvailable")

        val logicalPlan = query.lastExecution.logical

        // inspect the plan to confirm filter pushdown
        val scanRelations = logicalPlan.collect {
          case s: StreamingDataSourceV2ScanRelation => s
        }

        assert(scanRelations.nonEmpty,
          "Logical plan should contain a StreamingDataSourceV2ScanRelation")

        val scan = scanRelations.head.scan
        val pushed = scan.asInstanceOf[FakeScan].pushedFilters
        assert(pushed.exists(_.toString.contains("part")),
          "The 'part' filter was missing from pushed predicates")
      },
      StopStream
    )
  }
}

/**
 * Fake datasource
 */
class FakeStreamingProvider extends TableProvider {
  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: java.util.Map[String, String]): Table = {
    new FakeTable()
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType(Seq(
      StructField("id", org.apache.spark.sql.types.LongType),
      StructField("part", StringType)
    ))
  }
}

class FakeTable extends org.apache.spark.sql.connector.catalog.SupportsRead {
  override def name(): String = "fake_table"
  override def capabilities():
  java.util.Set[org.apache.spark.sql.connector.catalog.TableCapability] = {
    java.util.EnumSet.of(org.apache.spark.sql.connector.catalog.TableCapability.MICRO_BATCH_READ)
  }
  override def newScanBuilder(options: CaseInsensitiveStringMap):
    org.apache.spark.sql.connector.read.ScanBuilder = {
    new FakeScanBuilder()
  }
}

class FakeScan(val pushedFilters: Array[Filter]) extends Scan {
  override def readSchema(): StructType = null
  override def toMicroBatchStream(path: String): MicroBatchStream = new FakeMicroBatchStream()
}

class FakeScanBuilder extends ScanBuilder with SupportsPushDownFilters {
  private var pushed: Array[Filter] = Array.empty
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushed = filters
    Array.empty // all filters are pushed
  }
  override def pushedFilters(): Array[Filter] = pushed
  override def build(): Scan = new FakeScan(pushed)
}

class FakeMicroBatchStream extends MicroBatchStream {
  override def initialOffset(): Offset = LongOffset(0)
  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)
  override def latestOffset(): Offset = {
      LongOffset(1)
  }
  override def createReaderFactory(): PartitionReaderFactory = null
  override def stop(): Unit = {}
  override def planInputPartitions(start: streaming.Offset,
                                   end: streaming.Offset): Array[InputPartition] = Array.empty
  override def commit(end: streaming.Offset): Unit = {}
}
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
package org.apache.spark.sql.execution.datasources.v2

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class RangeInputPartition(start: Int, end: Int) extends InputPartition

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

abstract class SimpleBatchTable extends Table with SupportsRead  {

  override def schema(): StructType = TestingV2Source.schema
  override def name(): String = this.getClass.toString
  override def capabilities(): util.Set[TableCapability] = Set(BATCH_READ).asJava
}

object TestingV2Source {
  val schema = new StructType().add("i", "int").add("j", "int")
}

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

abstract class SimpleScanBuilder extends ScanBuilder
  with Batch with Scan {

  override def build(): Scan = this

  override def toBatch: Batch = this

  override def readSchema(): StructType = TestingV2Source.schema

  override def createReaderFactory(): PartitionReaderFactory = SimpleReaderFactory
}

class SimpleCustomMetric extends CustomMetric {
  override def name(): String = "custom_metric"
  override def description(): String = "a simple custom metric"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    s"custom_metric: ${taskMetrics.mkString(", ")}"
  }
}

// The followings are for custom metrics of V2 data source.
object CustomMetricReaderFactory extends PartitionReaderFactory {
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

      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        val metric = new CustomTaskMetric {
          override def name(): String = "custom_metric"
          override def value(): Long = 12345
        }
        Array(metric)
      }
    }
  }
}

class CustomMetricScanBuilder extends SimpleScanBuilder {
  override def planInputPartitions(): Array[InputPartition] = {
    Array(RangeInputPartition(0, 5), RangeInputPartition(5, 10))
  }

  override def supportedCustomMetrics(): Array[CustomMetric] = {
    Array(new SimpleCustomMetric)
  }

  override def createReaderFactory(): PartitionReaderFactory = CustomMetricReaderFactory
}

class CustomMetricDataSourceV2 extends TestingV2Source {

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new CustomMetricScanBuilder()
    }
  }
}

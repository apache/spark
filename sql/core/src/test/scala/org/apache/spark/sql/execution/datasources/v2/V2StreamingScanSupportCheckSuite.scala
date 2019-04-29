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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{Offset, Source, StreamingRelation, StreamingRelationV2}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.sources.v2.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V2StreamingScanSupportCheckSuite extends SparkFunSuite with SharedSparkSession {
  import TableCapability._

  private def createStreamingRelation(table: Table, v1Relation: Option[StreamingRelation]) = {
    StreamingRelationV2(FakeTableProvider, "fake", table, CaseInsensitiveStringMap.empty(),
      FakeTableProvider.schema.toAttributes, v1Relation)(spark)
  }

  private def createStreamingRelationV1() = {
    StreamingRelation(DataSource(spark, classOf[FakeStreamSourceProvider].getName))
  }

  test("check correct plan") {
    val plan1 = createStreamingRelation(CapabilityTable(MICRO_BATCH_READ), None)
    val plan2 = createStreamingRelation(CapabilityTable(CONTINUOUS_READ), None)
    val plan3 = createStreamingRelation(CapabilityTable(MICRO_BATCH_READ, CONTINUOUS_READ), None)
    val plan4 = createStreamingRelationV1()

    V2StreamingScanSupportCheck(Union(plan1, plan1))
    V2StreamingScanSupportCheck(Union(plan2, plan2))
    V2StreamingScanSupportCheck(Union(plan1, plan3))
    V2StreamingScanSupportCheck(Union(plan2, plan3))
    V2StreamingScanSupportCheck(Union(plan1, plan4))
    V2StreamingScanSupportCheck(Union(plan3, plan4))
  }

  test("table without scan capability") {
    val e = intercept[AnalysisException] {
      V2StreamingScanSupportCheck(createStreamingRelation(CapabilityTable(), None))
    }
    assert(e.message.contains("does not support either micro-batch or continuous scan"))
  }

  test("mix micro-batch only and continuous only") {
    val plan1 = createStreamingRelation(CapabilityTable(MICRO_BATCH_READ), None)
    val plan2 = createStreamingRelation(CapabilityTable(CONTINUOUS_READ), None)

    val e = intercept[AnalysisException] {
      V2StreamingScanSupportCheck(Union(plan1, plan2))
    }
    assert(e.message.contains(
      "The streaming sources in a query do not have a common supported execution mode"))
  }

  test("mix continuous only and v1 relation") {
    val plan1 = createStreamingRelation(CapabilityTable(CONTINUOUS_READ), None)
    val plan2 = createStreamingRelationV1()
    val e = intercept[AnalysisException] {
      V2StreamingScanSupportCheck(Union(plan1, plan2))
    }
    assert(e.message.contains(
      "The streaming sources in a query do not have a common supported execution mode"))
  }
}

private object FakeTableProvider extends TableProvider {
  val schema = new StructType().add("i", "int")

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException
  }
}

private case class CapabilityTable(_capabilities: TableCapability*) extends Table {
  override def name(): String = "capability_test_table"
  override def schema(): StructType = FakeTableProvider.schema
  override def capabilities(): util.Set[TableCapability] = _capabilities.toSet.asJava
}

private class FakeStreamSourceProvider extends StreamSourceProvider {
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    "fake" -> FakeTableProvider.schema
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    new Source {
      override def schema: StructType = FakeTableProvider.schema
      override def getOffset: Option[Offset] = {
        throw new UnsupportedOperationException
      }
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        throw new UnsupportedOperationException
      }
      override def stop(): Unit = {}
    }
  }
}

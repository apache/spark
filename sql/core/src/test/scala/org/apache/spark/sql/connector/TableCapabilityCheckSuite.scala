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

package org.apache.spark.sql.connector

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.analysis.{AnalysisSuite, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, TableCapabilityCheck}
import org.apache.spark.sql.execution.streaming.{Offset, Source, StreamingRelation}
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TableCapabilityCheckSuite extends AnalysisSuite with SharedSparkSession {

  private val emptyMap = CaseInsensitiveStringMap.empty
  private def createStreamingRelation(table: Table, v1Relation: Option[StreamingRelation]) = {
    StreamingRelationV2(
      Some(new FakeV2Provider),
      "fake",
      table,
      CaseInsensitiveStringMap.empty(),
      TableCapabilityCheckSuite.schema.toAttributes,
      None,
      None,
      v1Relation)
  }

  private def createStreamingRelationV1() = {
    StreamingRelation(DataSource(spark, classOf[TestStreamSourceProvider].getName))
  }

  test("batch scan: check missing capabilities") {
    val e = intercept[AnalysisException] {
      TableCapabilityCheck.apply(
        DataSourceV2Relation.create(CapabilityTable(), None, None, emptyMap)
      )
    }
    assert(e.message.contains("does not support batch scan"))
  }

  test("streaming scan: check missing capabilities") {
    val e = intercept[AnalysisException] {
      TableCapabilityCheck.apply(createStreamingRelation(CapabilityTable(), None))
    }
    assert(e.message.contains("does not support either micro-batch or continuous scan"))
  }

  test("streaming scan: mix micro-batch sources and continuous sources") {
    val microBatchOnly = createStreamingRelation(CapabilityTable(MICRO_BATCH_READ), None)
    val continuousOnly = createStreamingRelation(CapabilityTable(CONTINUOUS_READ), None)
    val both = createStreamingRelation(CapabilityTable(MICRO_BATCH_READ, CONTINUOUS_READ), None)
    val v1Source = createStreamingRelationV1()

    TableCapabilityCheck.apply(Union(microBatchOnly, microBatchOnly))
    TableCapabilityCheck.apply(Union(continuousOnly, continuousOnly))
    TableCapabilityCheck.apply(Union(both, microBatchOnly))
    TableCapabilityCheck.apply(Union(both, continuousOnly))
    TableCapabilityCheck.apply(Union(both, v1Source))

    val e = intercept[AnalysisException] {
      TableCapabilityCheck.apply(Union(microBatchOnly, continuousOnly))
    }
    assert(e.getMessage.contains(
      "The streaming sources in a query do not have a common supported execution mode"))
  }

  test("AppendData: check missing capabilities") {
    val plan = AppendData.byName(
      DataSourceV2Relation.create(CapabilityTable(), None, None, emptyMap),
      TestRelation)

    val exc = intercept[AnalysisException]{
      TableCapabilityCheck.apply(plan)
    }

    assert(exc.getMessage.contains("does not support append in batch mode"))
  }

  test("AppendData: check correct capabilities") {
    Seq(BATCH_WRITE, V1_BATCH_WRITE).foreach { write =>
      val plan = AppendData.byName(
        DataSourceV2Relation.create(CapabilityTable(write), None, None, emptyMap),
        TestRelation)

      TableCapabilityCheck.apply(plan)
    }
  }

  test("Truncate: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(V1_BATCH_WRITE),
      CapabilityTable(TRUNCATE),
      CapabilityTable(OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, None, None, emptyMap),
        TestRelation,
        Literal(true))

      val exc = intercept[AnalysisException]{
        TableCapabilityCheck.apply(plan)
      }

      assert(exc.getMessage.contains("does not support truncate in batch mode"))
    }
  }

  test("Truncate: check correct capabilities") {
    Seq(CapabilityTable(BATCH_WRITE, TRUNCATE),
      CapabilityTable(V1_BATCH_WRITE, TRUNCATE),
      CapabilityTable(BATCH_WRITE, OVERWRITE_BY_FILTER),
      CapabilityTable(V1_BATCH_WRITE, OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, None, None, emptyMap),
        TestRelation,
        Literal(true))

      TableCapabilityCheck.apply(plan)
    }
  }

  test("OverwriteByExpression: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(V1_BATCH_WRITE),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, None, None, emptyMap),
        TestRelation,
        EqualTo(AttributeReference("x", LongType)(), Literal(5)))

      val exc = intercept[AnalysisException]{
        TableCapabilityCheck.apply(plan)
      }

      assert(exc.getMessage.contains("does not support overwrite by filter in batch mode"))
    }
  }

  test("OverwriteByExpression: check correct capabilities") {
    Seq(BATCH_WRITE, V1_BATCH_WRITE).foreach { write =>
      val table = CapabilityTable(write, OVERWRITE_BY_FILTER)
      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, None, None, emptyMap),
        TestRelation,
        EqualTo(AttributeReference("x", LongType)(), Literal(5)))

      TableCapabilityCheck.apply(plan)
    }
  }

  test("OverwritePartitionsDynamic: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(OVERWRITE_DYNAMIC)).foreach { table =>

      val plan = OverwritePartitionsDynamic.byName(
        DataSourceV2Relation.create(table, None, None, emptyMap),
        TestRelation)

      val exc = intercept[AnalysisException] {
        TableCapabilityCheck.apply(plan)
      }

      assert(exc.getMessage.contains("does not support dynamic overwrite in batch mode"))
    }
  }

  test("OverwritePartitionsDynamic: check correct capabilities") {
    val table = CapabilityTable(BATCH_WRITE, OVERWRITE_DYNAMIC)
    val plan = OverwritePartitionsDynamic.byName(
      DataSourceV2Relation.create(table, None, None, emptyMap),
      TestRelation)

    TableCapabilityCheck.apply(plan)
  }
}

private object TableCapabilityCheckSuite {
  val schema: StructType = new StructType().add("id", LongType).add("data", StringType)
}

private case object TestRelation extends LeafNode with NamedRelation {
  override def name: String = "source_relation"
  override def output: Seq[AttributeReference] = TableCapabilityCheckSuite.schema.toAttributes
}

private case class CapabilityTable(_capabilities: TableCapability*) extends Table {
  override def name(): String = "capability_test_table"
  override def schema(): StructType = TableCapabilityCheckSuite.schema
  override def capabilities(): util.Set[TableCapability] = _capabilities.toSet.asJava
}

private class TestStreamSourceProvider extends StreamSourceProvider {
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    "test" -> TableCapabilityCheckSuite.schema
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    new Source {
      override def schema: StructType = TableCapabilityCheckSuite.schema
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

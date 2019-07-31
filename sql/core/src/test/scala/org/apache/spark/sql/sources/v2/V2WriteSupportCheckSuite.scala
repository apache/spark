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

package org.apache.spark.sql.sources.v2

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LeafNode, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2WriteSupportCheck}
import org.apache.spark.sql.sources.v2.TableCapability._
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V2WriteSupportCheckSuite extends AnalysisTest {

  test("AppendData: check missing capabilities") {
    val plan = AppendData.byName(
      DataSourceV2Relation.create(CapabilityTable(), CaseInsensitiveStringMap.empty), TestRelation)

    val exc = intercept[AnalysisException]{
      V2WriteSupportCheck.apply(plan)
    }

    assert(exc.getMessage.contains("does not support append in batch mode"))
  }

  test("AppendData: check correct capabilities") {
    val plan = AppendData.byName(
      DataSourceV2Relation.create(CapabilityTable(BATCH_WRITE), CaseInsensitiveStringMap.empty),
      TestRelation)

    V2WriteSupportCheck.apply(plan)
  }

  test("Truncate: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(TRUNCATE),
      CapabilityTable(OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation,
        Literal(true))

      val exc = intercept[AnalysisException]{
        V2WriteSupportCheck.apply(plan)
      }

      assert(exc.getMessage.contains("does not support truncate in batch mode"))
    }
  }

  test("Truncate: check correct capabilities") {
    Seq(CapabilityTable(BATCH_WRITE, TRUNCATE),
      CapabilityTable(BATCH_WRITE, OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation,
        Literal(true))

      V2WriteSupportCheck.apply(plan)
    }
  }

  test("OverwriteByExpression: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation,
        EqualTo(AttributeReference("x", LongType)(), Literal(5)))

      val exc = intercept[AnalysisException]{
        V2WriteSupportCheck.apply(plan)
      }

      assert(exc.getMessage.contains(
        "does not support overwrite expression (`x` = 5) in batch mode"))
    }
  }

  test("OverwriteByExpression: check correct capabilities") {
    val table = CapabilityTable(BATCH_WRITE, OVERWRITE_BY_FILTER)
    val plan = OverwriteByExpression.byName(
      DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation,
      EqualTo(AttributeReference("x", LongType)(), Literal(5)))

    V2WriteSupportCheck.apply(plan)
  }

  test("OverwritePartitionsDynamic: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(OVERWRITE_DYNAMIC)).foreach { table =>

      val plan = OverwritePartitionsDynamic.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation)

      val exc = intercept[AnalysisException] {
        V2WriteSupportCheck.apply(plan)
      }

      assert(exc.getMessage.contains("does not support dynamic overwrite in batch mode"))
    }
  }

  test("OverwritePartitionsDynamic: check correct capabilities") {
    val table = CapabilityTable(BATCH_WRITE, OVERWRITE_DYNAMIC)
    val plan = OverwritePartitionsDynamic.byName(
      DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation)

    V2WriteSupportCheck.apply(plan)
  }
}

private object V2WriteSupportCheckSuite {
  val schema: StructType = new StructType().add("id", LongType).add("data", StringType)
}

private case object TestRelation extends LeafNode with NamedRelation {
  override def name: String = "source_relation"
  override def output: Seq[AttributeReference] = V2WriteSupportCheckSuite.schema.toAttributes
}

private case class CapabilityTable(_capabilities: TableCapability*) extends Table {
  override def name(): String = "capability_test_table"
  override def schema(): StructType = V2WriteSupportCheckSuite.schema
  override def capabilities(): util.Set[TableCapability] = _capabilities.toSet.asJava
}

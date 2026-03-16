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

import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.expressions.aggregate.GeneralAggregateFunc
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, V1ScanWrapper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

trait DataSourcePushdownTestUtils extends ExplainSuiteHelper {
  protected def checkSamplePushed(df: DataFrame, pushed: Boolean = true): Unit = {
    val sample = df.queryExecution.optimizedPlan.collect {
      case s: Sample => s
    }
    if (pushed) {
      assert(sample.isEmpty)
    } else {
      assert(sample.nonEmpty)
    }
  }

  protected def checkFilterPushed(df: DataFrame, pushed: Boolean = true): Unit = {
    val filter = df.queryExecution.optimizedPlan.collect {
      case f: Filter => f
    }
    if (pushed) {
      assert(filter.isEmpty)
    } else {
      assert(filter.nonEmpty)
    }
  }

  protected def checkLimitRemoved(df: DataFrame, pushed: Boolean = true): Unit = {
    val limit = df.queryExecution.optimizedPlan.collect {
      case l: LocalLimit => l
      case g: GlobalLimit => g
    }
    if (pushed) {
      assert(limit.isEmpty)
    } else {
      assert(limit.nonEmpty)
    }
  }

  protected def checkLimitPushed(df: DataFrame, limit: Option[Int]): Unit = {
    df.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation => relation.scan match {
        case v1: V1ScanWrapper =>
          assert(v1.pushedDownOperators.limit == limit)
      }
    }
  }

  protected def checkColumnPruned(df: DataFrame, col: String): Unit = {
    val scan = df.queryExecution.optimizedPlan.collectFirst {
      case s: DataSourceV2ScanRelation => s
    }.get
    assert(scan.schema.names.sameElements(Seq(col)))
  }

  protected def checkAggregateRemoved(df: DataFrame, pushed: Boolean = true): Unit = {
    val aggregates = df.queryExecution.optimizedPlan.collect {
      case agg: Aggregate => agg
    }
    if (pushed) {
      assert(aggregates.isEmpty)
    } else {
      assert(aggregates.nonEmpty)
    }
  }

  protected def checkAggregatePushed(df: DataFrame, funcName: String): Unit = {
    df.queryExecution.optimizedPlan.collect {
      case DataSourceV2ScanRelation(_, scan, _, _, _) =>
        assert(scan.isInstanceOf[V1ScanWrapper])
        val wrapper = scan.asInstanceOf[V1ScanWrapper]
        assert(wrapper.pushedDownOperators.aggregation.isDefined)
        val aggregationExpressions =
          wrapper.pushedDownOperators.aggregation.get.aggregateExpressions()
        assert(aggregationExpressions.exists { expr =>
          expr.isInstanceOf[GeneralAggregateFunc] &&
            expr.asInstanceOf[GeneralAggregateFunc].name() == funcName
        })
    }
  }

  protected def checkSortRemoved(
      df: DataFrame,
      pushed: Boolean = true): Unit = {
    val sorts = df.queryExecution.optimizedPlan.collect {
      case s: Sort => s
    }

    if (pushed) {
      assert(sorts.isEmpty)
    } else {
      assert(sorts.nonEmpty)
    }
  }

  protected def checkOffsetRemoved(
      df: DataFrame,
      pushed: Boolean = true): Unit = {
    val offsets = df.queryExecution.optimizedPlan.collect {
      case o: Offset => o
    }

    if (pushed) {
      assert(offsets.isEmpty)
    } else {
      assert(offsets.nonEmpty)
    }
  }

  protected def checkOffsetPushed(df: DataFrame, offset: Option[Int]): Unit = {
    df.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation => relation.scan match {
        case v1: V1ScanWrapper =>
          assert(v1.pushedDownOperators.offset == offset)
      }
    }
  }

  protected def checkJoinNotPushed(df: DataFrame): Unit = {
    val joinNodes = df.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }
    assert(joinNodes.nonEmpty, "Join should not be pushed down")
  }

  protected def checkJoinPushed(df: DataFrame): Unit = {
    val joinNodes = df.queryExecution.optimizedPlan.collect {
      case j: Join => j
    }
    assert(joinNodes.isEmpty, "Join should be pushed down")
  }

  protected def checkJoinPushed(df: DataFrame, expectedPushdownString: String): Unit = {
    checkJoinPushed(df)
    if (expectedPushdownString.nonEmpty) {
      checkPushedInfo(df, expectedPushdownString)
    }
  }

  protected def checkPushedInfo(df: DataFrame, expectedPlanFragment: String*): Unit = {
    withSQLConf(SQLConf.MAX_METADATA_STRING_LENGTH.key -> "1000") {
      df.queryExecution.optimizedPlan.collect {
        case _: DataSourceV2ScanRelation =>
          checkKeywordsExistsInExplain(df, expectedPlanFragment: _*)
      }
    }
  }

  /**
   * Check if the output schema of dataframe {@code df} is same as {@code schema}. There is one
   * limitation: if expected schema name is empty, assertion on same names will be skipped.
   * <br>
   * For example, it is not really possible to use {@code checkPrunedColumns} for join pushdown,
   * because in case of duplicate names, columns will have random UUID suffixes. For this reason,
   * the best we can do is test that the size is same, and other fields beside names do match.
   */
  protected def checkPrunedColumnsDataTypeAndNullability(
      df: DataFrame,
      schema: StructType): Unit = {
    df.queryExecution.optimizedPlan.collect {
      case relation: DataSourceV2ScanRelation => relation.scan match {
        case v1: V1ScanWrapper =>
          val dfSchema = v1.readSchema()

          assert(dfSchema.length == schema.length)
          dfSchema.fields.zip(schema.fields).foreach { case (f1, f2) =>
            assert(f1.name == f2.name)
            assert(f1.dataType == f2.dataType)
            assert(f1.nullable == f2.nullable)
          }
      }
    }
  }
}

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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LocalLimit, Project, Sort}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class SpecialLimitsSuite
  extends SparkPlanTest
  with SharedSparkSession {
  import testImplicits._

  private val tableFormat: String = "parquet"

  test("SPARK-40501: Enhance 'SpecialLimits' to support project(..., limit(...))") {
    Seq(true, false).foreach { codegenEnabled =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString) {
        withTable("tbl") {
          (1 to 3).map(i => (i, i.toString)).toDF("a", "b")
            .write
            .format(tableFormat)
            .mode("overwrite")
            .saveAsTable("tbl")

          val df1 = sql("select * from tbl limit 1")
          val castCols1 = df1.logicalPlan.output.map { col =>
            Column(col).cast(StringType)
          }
          val res1 = df1.select(castCols1: _*).limit(21)
          val queryExecution1 = res1.queryExecution

          val optimizedLogicalPlan1 = queryExecution1.optimizedPlan
          val logicalPlanCheck1 = optimizedLogicalPlan1 match {
            case Project(_, GlobalLimit(_, LocalLimit(_, _))) => true
            case _ => false
          }
          assert(logicalPlanCheck1)

          val physicalPlan1 = queryExecution1.executedPlan
          val physicalPlanCheck1 = (physicalPlan1, codegenEnabled) match {
            case (CollectLimitExec(_, WholeStageCodegenExec(ProjectExec(_, _)), _), true) => true
            case (CollectLimitExec(_, ProjectExec(_, _), _), false) => true
            case _ => false
          }
          assert(physicalPlanCheck1)

          val df2 = sql("select * from tbl order by a limit 1")
          val castCols2 = df2.logicalPlan.output.map { col =>
            Column(col).cast(StringType)
          }
          val res2 = df2.select(castCols2: _*).limit(21)
          val queryExecution2 = res2.queryExecution
          val optimizedLogicalPlan2 = queryExecution2.optimizedPlan
          val logicalPlanCheck2 = optimizedLogicalPlan2 match {
            case Project(_, GlobalLimit(_, LocalLimit(_, Sort(_, _, _)))) => true
            case _ => false
          }
          assert(logicalPlanCheck2)

          val physicalPlan2 = queryExecution2.executedPlan
          val physicalPlanCheck2 = (physicalPlan2, codegenEnabled) match {
            case (TakeOrderedAndProjectExec(_, _, _,
                WholeStageCodegenExec(ColumnarToRowExec(InputAdapter(
                  FileSourceScanExec(_, _, _, _, _, _, _, _, _)))), _), true) => true
            case (TakeOrderedAndProjectExec(_, _, _,
                FileSourceScanExec(_, _, _, _, _, _, _, _, _), _), false) => true
            case _ => false
          }
          assert(physicalPlanCheck2)
        }
      }
    }
  }
}

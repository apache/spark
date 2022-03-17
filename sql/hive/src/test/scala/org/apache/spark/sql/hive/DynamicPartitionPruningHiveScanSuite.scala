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

package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

abstract class DynamicPartitionPruningHiveScanSuiteBase
    extends DynamicPartitionPruningSuiteBase with TestHiveSingleton with SQLTestUtils {

  override val tableFormat: String = "hive"

  override protected def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    flatMap(plan) {
      case s: FileSourceScanExec => s.partitionFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case h: HiveTableScanExec => h.partitionPruningPred.collect {
        case d: DynamicPruningExpression => d.child
      }
      case _ => Nil
    }
  }
}

class DynamicPartitionPruningHiveScanSuiteAEOff extends DynamicPartitionPruningHiveScanSuiteBase
  with DisableAdaptiveExecutionSuite

class DynamicPartitionPruningHiveScanSuiteAEOn extends DynamicPartitionPruningHiveScanSuiteBase
  with EnableAdaptiveExecutionSuite

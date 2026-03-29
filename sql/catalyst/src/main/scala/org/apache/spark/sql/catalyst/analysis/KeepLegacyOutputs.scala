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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{DescribeNamespace, LogicalPlan, ShowTableProperties, ShowTables}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule for keeping the SQL command's legacy outputs.
 */
object KeepLegacyOutputs extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA)) {
      plan
    } else {
      plan.resolveOperatorsUpWithPruning(
        _.containsPattern(COMMAND)) {
        case s: ShowTables =>
          assert(s.output.length == 3)
          val newOutput = s.output.head.withName("database") +: s.output.tail
          s.copy(output = newOutput)
        case d: DescribeNamespace =>
          assert(d.output.length == 2)
          d.copy(output = Seq(d.output.head.withName("database_description_item"),
            d.output.last.withName("database_description_value")))
        case s: ShowTableProperties if s.propertyKey.isDefined =>
           s.copy(output = Seq(s.output.last))
      }
    }
  }
}

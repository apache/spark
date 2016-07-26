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

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

/**
 * An analyzer rule that handles function aliases.
 */
object SubstituteFunctionAliases extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressions {
    // SPARK-16730: The following functions are aliases for cast in Hive.
    case u: UnresolvedFunction
        if u.name.database.isEmpty && u.children.size == 1 && !u.isDistinct =>
      u.name.funcName.toLowerCase match {
        case "boolean" => Cast(u.children.head, BooleanType)
        case "tinyint" => Cast(u.children.head, ByteType)
        case "smallint" => Cast(u.children.head, ShortType)
        case "int" => Cast(u.children.head, IntegerType)
        case "bigint" => Cast(u.children.head, LongType)
        case "float" => Cast(u.children.head, FloatType)
        case "double" => Cast(u.children.head, DoubleType)
        case "decimal" => Cast(u.children.head, DecimalType.USER_DEFAULT)
        case "date" => Cast(u.children.head, DateType)
        case "timestamp" => Cast(u.children.head, TimestampType)
        case "binary" => Cast(u.children.head, BinaryType)
        case "string" => Cast(u.children.head, StringType)
        case _ => u
      }
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableCommand, AlterViewAs, AlterViewSchemaBinding, ColumnDefinition, CreateFunction, CreateTable, CreateTableAsSelect, CreateView, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ImplicitStringType, StringType}

object ResolveImplicitStringTypes extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _: CreateTable | _: CreateTableAsSelect | _: AlterTableCommand |
           _: CreateView | _: AlterViewAs | _: AlterViewSchemaBinding | _: AlterViewSchemaBinding |
           _: CreateFunction =>
        val res = replaceWith(plan, StringType)
        res

      case _ =>
        val res = replaceWith(plan, SQLConf.get.defaultStringType)
        res
    }
  }

  private def replaceWith(plan: LogicalPlan, newType: StringType): LogicalPlan = {
    plan resolveOperators {
      case l: LogicalPlan =>
        l transformExpressions {
          case columnDef: ColumnDefinition if columnDef.dataType == ImplicitStringType =>
            columnDef.copy(dataType = newType)

          case Literal(value, ImplicitStringType) =>
            Literal(value, newType)

          case cast: Cast if cast.dataType == ImplicitStringType =>
            cast.copy(dataType = newType)
        }
    }
  }
}

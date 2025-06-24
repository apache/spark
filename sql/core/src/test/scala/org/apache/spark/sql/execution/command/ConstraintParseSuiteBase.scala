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
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.expressions.{ConstraintCharacteristic, TableConstraint}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.{ColumnDefinition, CreateTable, LogicalPlan, OptionList, ReplaceTable, UnresolvedTableSpec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

abstract class ConstraintParseSuiteBase extends AnalysisTest with SharedSparkSession {
  protected def validConstraintCharacteristics = Seq(
    ("", "", ConstraintCharacteristic(enforced = None, rely = None)),
    ("", "RELY", ConstraintCharacteristic(enforced = None, rely = Some(true))),
    ("", "NORELY", ConstraintCharacteristic(enforced = None, rely = Some(false)))
  )

  protected def enforcedConstraintCharacteristics = Seq(
    ("ENFORCED", "", ConstraintCharacteristic(enforced = Some(true), rely = None)),
    ("ENFORCED", "RELY", ConstraintCharacteristic(enforced = Some(true), rely = Some(true))),
    ("ENFORCED", "NORELY", ConstraintCharacteristic(enforced = Some(true), rely = Some(false))),
    ("RELY", "ENFORCED", ConstraintCharacteristic(enforced = Some(true), rely = Some(true))),
    ("NORELY", "ENFORCED", ConstraintCharacteristic(enforced = Some(true), rely = Some(false)))
  )

  protected def notEnforcedConstraintCharacteristics = Seq(
    ("NOT ENFORCED", "RELY",
      ConstraintCharacteristic(enforced = Some(false), rely = Some(true))),
    ("NOT ENFORCED", "NORELY",
      ConstraintCharacteristic(enforced = Some(false), rely = Some(false))),
    ("RELY", "NOT ENFORCED",
      ConstraintCharacteristic(enforced = Some(false), rely = Some(true))),
    ("NORELY", "NOT ENFORCED",
      ConstraintCharacteristic(enforced = Some(false), rely = Some(false))),
    ("NOT ENFORCED", "",
      ConstraintCharacteristic(enforced = Some(false), rely = None))
  )

  protected val invalidConstraintCharacteristics = Seq(
    ("ENFORCED", "ENFORCED"),
    ("ENFORCED", "NOT ENFORCED"),
    ("NOT ENFORCED", "ENFORCED"),
    ("NOT ENFORCED", "NOT ENFORCED"),
    ("RELY", "RELY"),
    ("RELY", "NORELY"),
    ("NORELY", "RELY"),
    ("NORELY", "NORELY")
  )

  protected def createExpectedPlan(
      columns: Seq[ColumnDefinition],
      tableConstraints: Seq[TableConstraint],
      isCreateTable: Boolean = true): LogicalPlan = {
    val tableId = UnresolvedIdentifier(Seq("t"))
    val tableSpec = UnresolvedTableSpec(
      Map.empty[String, String], Some("parquet"), OptionList(Seq.empty),
      None, None, None, None, false, tableConstraints)
    if (isCreateTable) {
      CreateTable(tableId, columns, Seq.empty, tableSpec, false)
    } else {
      ReplaceTable(tableId, columns, Seq.empty, tableSpec, false)
    }
  }

  protected def verifyConstraints(
      sql: String,
      constraints: Seq[TableConstraint],
      isCreateTable: Boolean = true): Unit = {
    val parsed = parsePlan(sql)
    val columns = Seq(
      ColumnDefinition("a", IntegerType),
      ColumnDefinition("b", StringType)
    )
    val expected = createExpectedPlan(
      columns = columns, tableConstraints = constraints, isCreateTable = isCreateTable)
    comparePlans(parsed, expected)
  }
}

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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CollationFactory.CollationMeta
import org.apache.spark.sql.types.StringType

/**
 * A command for `SHOW COLLATIONS`.
 *
 * The syntax of this command is:
 * {{{
 *    SHOW identifier? COLLATIONS ((FROM | IN) ns=identifierReference)? (LIKE? pattern=stringLit);
 * }}}
 */
case class ShowCollationsCommand(
    ns: LogicalPlan,
    userScope: Boolean,
    systemScope: Boolean,
    pattern: Option[String]) extends UnaryRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("COLLATION_CATALOG", StringType, nullable = false)(),
    AttributeReference("COLLATION_SCHEMA", StringType, nullable = false)(),
    AttributeReference("COLLATION_NAME", StringType, nullable = false)(),
    AttributeReference("LANGUAGE", StringType)(),
    AttributeReference("COUNTRY", StringType)(),
    AttributeReference("ACCENT_SENSITIVITY", StringType, nullable = false)(),
    AttributeReference("CASE_SENSITIVITY", StringType, nullable = false)(),
    AttributeReference("PAD_ATTRIBUTE", StringType, nullable = false)(),
    AttributeReference("ICU_VERSION", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val systemCollations: Seq[CollationMeta] = if (systemScope) {
      sparkSession.sessionState.catalog.listCollations(pattern)
    } else Seq.empty

    systemCollations.map(m => Row(
      m.catalog,
      m.schema,
      m.collationName,
      m.language,
      m.country,
      if (m.accentSensitivity) "ACCENT_SENSITIVE" else "ACCENT_INSENSITIVE",
      if (m.caseSensitivity) "CASE_SENSITIVE" else "CASE_INSENSITIVE",
      m.padAttribute,
      m.icuVersion
    ))
  }

  override def child: LogicalPlan = ns

  override protected def withNewChildInternal(
      newChild: LogicalPlan): ShowCollationsCommand =
    copy(ns = newChild)
}

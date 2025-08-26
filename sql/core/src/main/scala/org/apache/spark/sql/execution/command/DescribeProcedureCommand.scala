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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, ProcedureCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StringType

/**
 * A command for users to describe a procedure.
 * The syntax of using this command in SQL is:
 * {{{
 *   DESC PROCEDURE procedure_name
 * }}}
 */
case class DescribeProcedureCommand(
    child: LogicalPlan,
    override val output: Seq[Attribute] = Seq(
      AttributeReference("procedure_desc", StringType, nullable = false)()
    )) extends UnaryRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    child match {
      case ResolvedIdentifier(catalog, ident) =>
        val procedure = load(catalog.asProcedureCatalog, ident)
        describeV2Procedure(procedure)
      case _ =>
        throw SparkException.internalError(s"Invalid procedure identifier: ${child.getClass}")
    }
  }

  private def load(catalog: ProcedureCatalog, ident: Identifier): UnboundProcedure = {
    try {
      catalog.loadProcedure(ident)
    } catch {
      case e: Exception if !e.isInstanceOf[SparkThrowable] =>
        val nameParts = catalog.name +: ident.asMultipartIdentifier
        throw QueryCompilationErrors.failedToLoadRoutineError(nameParts, e)
    }
  }

  private def describeV2Procedure(procedure: UnboundProcedure): Seq[Row] = {
    val buffer = new ArrayBuffer[(String, String)]
    append(buffer, "Procedure:", procedure.name())
    append(buffer, "Description:", procedure.description())

    val keys = tabulate(buffer.map(_._1).toSeq)
    val values = buffer.map(_._2)
    keys.zip(values).map { case (key, value) => Row(s"$key $value") }
  }

  private def append(buffer: ArrayBuffer[(String, String)], key: String, value: String): Unit = {
    buffer += (key -> value)
  }

  /**
   * Pad all input strings into the same length using the max string length among all inputs.
   */
  private def tabulate(inputs: Seq[String]): Seq[String] = {
    val maxLen = inputs.map(_.length).max
    inputs.map { input => input.padTo(maxLen, " ").mkString }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

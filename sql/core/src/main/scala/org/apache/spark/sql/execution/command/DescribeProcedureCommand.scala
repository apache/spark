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

import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedProcedure
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.procedures.{ProcedureParameter, SimpleProcedure, UnboundProcedure}
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
      case ResolvedProcedure(catalog, ident, procedure: UnboundProcedure) =>
        describeV2Procedure(procedure)
      case _ =>
        throw SparkException.internalError(s"Invalid procedure identifier: ${child.getClass}")
    }
  }

  private def describeV2Procedure(procedure: UnboundProcedure): Seq[Row] = {
    val buffer = new ArrayBuffer[(String, String)]
    append(buffer, "Procedure:", procedure.name())
    append(buffer, "Description:", procedure.description())

    procedure match {
      case p: SimpleProcedure =>
        val params = p.parameters()
        if (params != null && params.nonEmpty) {
          val formattedParams = formatProcedureParameters(params)
          append(buffer, "Parameters:", formattedParams.head)
          formattedParams.tail.foreach(s => append(buffer, "", s))
        } else {
          append(buffer, "Parameters:", "()")
        }
      case _ =>
        // Do not show parameters for non-simple procedures
    }

    val keys = tabulate(buffer.map(_._1).toSeq)
    val values = buffer.map(_._2)
    keys.zip(values).map { case (key, value) => Row(s"$key $value") }
  }

  private def formatProcedureParameters(params: Array[ProcedureParameter]): Seq[String] = {
    val paramsStrings = params.map { p =>
      val mode = p.mode().toString
      val name = p.name()
      val dataType = p.dataType().sql
      val comment = if (p.comment() != null) s" '${p.comment()}'" else ""
      val defaultVal = if (p.defaultValue() != null) p.defaultValue().getSql else null
      val default = if (defaultVal != null) s" DEFAULT $defaultVal" else ""
      (mode, name, dataType, default, comment)
    }

    val modeLen = paramsStrings.map(_._1.length).max
    val nameLen = paramsStrings.map(_._2.length).max
    val dataTypeLen = paramsStrings.map(_._3.length).max

    paramsStrings.map { case (mode, name, dataType, default, comment) =>
      val paddedMode = mode.padTo(modeLen, " ").mkString
      val paddedName = name.padTo(nameLen, " ").mkString
      val paddedDataType = dataType.padTo(dataTypeLen, " ").mkString
      s"$paddedMode $paddedName $paddedDataType$default$comment"
    }.toSeq
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

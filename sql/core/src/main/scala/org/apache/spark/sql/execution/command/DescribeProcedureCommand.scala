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
import org.apache.spark.sql.connector.catalog.procedures.{ProcedureParameter, UnboundProcedure}
import org.apache.spark.sql.types.{StringType, StructType}

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
      case ResolvedProcedure(catalog, ident, procedure) =>
        describeV2Procedure(procedure.asInstanceOf[UnboundProcedure])
      case _ =>
        throw SparkException.internalError(s"Invalid procedure identifier: ${child.getClass}")
    }
  }


  private def describeV2Procedure(procedure: UnboundProcedure): Seq[Row] = {
    val buffer = new ArrayBuffer[(String, String)]
    append(buffer, "Procedure:", procedure.name())
    append(buffer, "Description:", procedure.description())

    // UnboundProcedure requires binding to retrieve parameters. We try to bind with an empty
    // argument list to get the parameters. If the procedure requires arguments, binding might
    // fail. In that case, we suppress the exception and just show the procedure metadata
    // without parameters.
    try {
      val bound = procedure.bind(new StructType())
      val params = bound.parameters()
      if (params != null && params.nonEmpty) {
        val formattedParams = formatProcedureParameters(params)
        append(buffer, "Parameters:", formattedParams.head)
        formattedParams.tail.foreach(s => append(buffer, "", s))
      } else {
        append(buffer, "Parameters:", "()")
      }
    } catch {
      case _: Exception =>
        // Ignore if binding fails
    }

    val keys = tabulate(buffer.map(_._1).toSeq)
    val values = buffer.map(_._2)
    keys.zip(values).map { case (key, value) => Row(s"$key $value") }
  }

  // This helper is needed because the V2 Procedure API returns an array of ProcedureParameter,
  // which differs from the StructType used by internal stored procedures (handled by
  // formatParameters).
  private def formatProcedureParameters(params: Array[ProcedureParameter]): Seq[String] = {
    val modes = tabulate(params.map(_.mode().toString).toSeq)
    val names = tabulate(params.map(_.name()).toSeq)
    val dataTypes = tabulate(params.map(_.dataType().sql).toSeq)
    val comments = params.map { p =>
      if (p.comment() != null) s" '${p.comment()}'" else ""
    }
    val defaults = params.map { p =>
      val defaultVal = if (p.defaultValue() != null) p.defaultValue().getSql else null
      if (defaultVal != null) s" DEFAULT $defaultVal" else ""
    }
    modes zip names zip dataTypes zip defaults zip comments map {
      case ((((mode, name), dataType), default), comment) =>
        s"$mode $name $dataType$default$comment"
    }
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

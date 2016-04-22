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

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.{ExecutedCommand, HiveNativeCommand, SetCommand}
import org.apache.spark.sql.hive.execution.DescribeHiveTableCommand


/**
 * A [[QueryExecution]] with hive specific features.
 */
protected[hive] class HiveQueryExecution(ctx: SQLContext, logicalPlan: LogicalPlan)
  extends QueryExecution(ctx, logicalPlan) {

  /**
   * Returns the result as a hive compatible sequence of strings.  For native commands, the
   * execution is simply passed back to Hive.
   */
  def stringResult(): Seq[String] = executedPlan match {
    case ExecutedCommand(desc: DescribeHiveTableCommand) =>
      // If it is a describe command for a Hive table, we want to have the output format
      // be similar with Hive.
      desc.run(ctx).map {
        case Row(name: String, dataType: String, comment) =>
          Seq(name, dataType,
            Option(comment.asInstanceOf[String]).getOrElse(""))
            .map(s => String.format(s"%-20s", s))
            .mkString("\t")
      }
    case command: ExecutedCommand =>
      command.executeCollect().map(_.getString(0))

    case other =>
      val result: Seq[Seq[Any]] = other.executeCollectPublic().map(_.toSeq).toSeq
      // We need the types so we can output struct field names
      val types = analyzed.output.map(_.dataType)
      // Reformat to match hive tab delimited output.
      result.map(_.zip(types).map(HiveUtils.toHiveString)).map(_.mkString("\t")).toSeq
  }

  override def simpleString: String =
    logical match {
      case _: HiveNativeCommand => "<Native command: executed by Hive>"
      case _: SetCommand => "<SET command: executed by Hive, and noted by SQLContext>"
      case _ => super.simpleString
    }

}

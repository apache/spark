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

package org.apache.spark.sql.scripting

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.CompoundBody

/**
 * SQL scripting interpreter - builds SQL script execution plan.
 */
class SqlScriptingExecution(
    sqlScript: CompoundBody,
    session: SparkSession) extends Iterator[DataFrame] {

  private val executionPlan: Iterator[CompoundStatementExec] =
    SqlScriptingInterpreter().buildExecutionPlan(sqlScript, session)

  private var current = if (executionPlan.hasNext) Some(executionPlan.next()) else None

  override def hasNext: Boolean = {
    current.isDefined
  }

  override def next(): DataFrame = {
    if (!hasNext) {
      throw new NoSuchElementException("No more statements to execute")
    }
    moveCurrentToNextResult()
    current.get.asInstanceOf[SingleStatementExec].buildDataFrame(session)
  }

  private def moveCurrentToNextResult(): Unit = {
    while (current.isDefined && !current.get.isResult) {
      current.get match {
        case exec: SingleStatementExec =>
          exec.buildDataFrame(session).collect()
        case _ => // Do nothing
      }
      current = if (executionPlan.hasNext) Some(executionPlan.next()) else None
    }
  }

}
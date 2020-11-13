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

package org.apache.spark.sql.hive.thriftserver

import java.util.{ArrayList => JArrayList, Arrays, List => JList}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}


private[hive] class SparkSQLDriver(val context: SQLContext = SparkSQLEnv.sqlContext)
  extends Driver
  with Logging {

  private[hive] var tableSchema: Schema = _
  private[hive] var hiveResponse: Seq[String] = _

  override def init(): Unit = {
  }

  private def getResultSetSchema(query: QueryExecution): Schema = {
    val analyzed = query.analyzed
    logDebug(s"Result Schema: ${analyzed.output}")
    if (analyzed.output.isEmpty) {
      new Schema(Arrays.asList(new FieldSchema("Response code", "string", "")), null)
    } else {
      val fieldSchemas = analyzed.output.map { attr =>
        new FieldSchema(attr.name, attr.dataType.catalogString, "")
      }

      new Schema(fieldSchemas.asJava, null)
    }
  }

  override def run(command: String): CommandProcessorResponse = {
    // TODO unify the error code
    try {
      val substitutorCommand = SQLConf.withExistingConf(context.conf) {
        new VariableSubstitution().substitute(command)
      }
      context.sparkContext.setJobDescription(substitutorCommand)
      val execution = context.sessionState.executePlan(context.sql(command).logicalPlan)
      hiveResponse = SQLExecution.withNewExecutionId(execution) {
        hiveResultString(execution.executedPlan)
      }
      tableSchema = getResultSetSchema(execution)
      new CommandProcessorResponse(0)
    } catch {
        case ae: AnalysisException =>
          logDebug(s"Failed in [$command]", ae)
          new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(ae), null, ae)
        case cause: Throwable =>
          logError(s"Failed in [$command]", cause)
          new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(cause), null, cause)
    }
  }

  override def close(): Int = {
    hiveResponse = null
    tableSchema = null
    0
  }

  override def getResults(res: JList[_]): Boolean = {
    if (hiveResponse == null) {
      false
    } else {
      res.asInstanceOf[JArrayList[String]].addAll(hiveResponse.asJava)
      hiveResponse = null
      true
    }
  }

  override def getSchema: Schema = tableSchema

  override def destroy(): Unit = {
    super.destroy()
    hiveResponse = null
    tableSchema = null
  }
}

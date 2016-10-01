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

package org.apache.spark.sql

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


// TODO: don't swallow original stack trace if it exists

/**
 * :: DeveloperApi ::
 * Thrown when a query fails to analyze, usually because the query itself is invalid.
 */
@DeveloperApi
class AnalysisException protected[sql] (
    val message: String,
    val line: Option[Int] = None,
    val startPosition: Option[Int] = None,
    val plan: Option[LogicalPlan] = None,
    val cause: Option[Throwable] = None)
  extends Exception(message, cause.orNull) with Serializable {

  def withPosition(line: Option[Int], startPosition: Option[Int]): AnalysisException = {
    val newException = new AnalysisException(message, line, startPosition)
    newException.setStackTrace(getStackTrace)
    newException
  }

  override def getMessage: String = {
    val planAnnotation = plan.map(p => s";\n$p").getOrElse("")
    getSimpleMessage + planAnnotation
  }

  def getSimpleMessage: String = {
    val lineAnnotation = line.map(l => s" line $l").getOrElse("")
    val positionAnnotation = startPosition.map(p => s" pos $p").getOrElse("")
    s"$message;$lineAnnotation$positionAnnotation"
  }
}

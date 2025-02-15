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

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{QUERY_HINT, RELATION_NAME, UNSUPPORTED_HINT_REASON}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{HintErrorHandler, HintInfo}

/**
 * The hint error handler that logs warnings for each hint error.
 */
object HintErrorLogger extends HintErrorHandler with Logging {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def hintNotRecognized(name: String, parameters: Seq[Expression]): Unit = {
    logWarning(log"Unrecognized hint: " +
      log"${MDC(QUERY_HINT, hintToPrettyString(name, parameters))}")
  }

  override def hintRelationsNotFound(
      name: String, parameters: Seq[Any], invalidRelations: Set[Seq[String]]): Unit = {
    invalidRelations.foreach { ident =>
      logWarning(log"Count not find relation '${MDC(RELATION_NAME, ident.quoted)}' " +
        log"specified in hint '${MDC(QUERY_HINT, hintToPrettyString(name, parameters))}'.")
    }
  }

  override def joinNotFoundForJoinHint(hint: HintInfo): Unit = {
    logWarning(log"A join hint ${MDC(QUERY_HINT, hint)} is specified " +
      log"but it is not part of a join relation.")
  }

  override def joinHintNotSupported(hint: HintInfo, reason: String): Unit = {
    logWarning(log"Hint ${MDC(QUERY_HINT, hint)} is not supported in the query: " +
      log"${MDC(UNSUPPORTED_HINT_REASON, reason)}.")
  }

  override def hintOverridden(hint: HintInfo): Unit = {
    logWarning(log"Hint ${MDC(QUERY_HINT, hint)} is overridden by another hint " +
      log"and will not take effect.")
  }

  private def hintToPrettyString(name: String, parameters: Seq[Any]): String = {
    val prettyParameters = parameters.map {
      case a: UnresolvedAttribute => a.nameParts.mkString(".")
      case e: Any => e.toString
    }
    s"$name${prettyParameters.mkString("(", ", ", ")")}"
  }
}

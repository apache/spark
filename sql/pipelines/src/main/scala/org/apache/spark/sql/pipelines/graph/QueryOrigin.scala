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

package org.apache.spark.sql.pipelines.graph

import scala.util.control.{NonFatal, NoStackTrace}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.pipelines.Language

/**
 * Records information used to track the provenance of a given query to user code.
 *
 * @param language The language used by the user to define the query.
 * @param filePath Path to the file of the user code that defines the query.
 * @param sqlText The SQL text of the query.
 * @param line The line number of the query in the user code.
 *             Line numbers are 1-indexed.
 * @param startPosition The start position of the query in the user code.
 * @param objectType The type of the object that the query is associated with. (Table, View, etc)
 * @param objectName The name of the object that the query is associated with.
 */
case class QueryOrigin(
    language: Option[Language] = None,
    filePath: Option[String] = None,
    sqlText: Option[String] = None,
    line: Option[Int] = None,
    startPosition: Option[Int] = None,
    objectType: Option[String] = None,
    objectName: Option[String] = None
) {

  /**
   * Merges this origin with another one.
   *
   * The result has fields set to the value in the other origin if it is defined, or if not, then
   * the value in this origin.
   */
  def merge(other: QueryOrigin): QueryOrigin = {
    QueryOrigin(
      language = other.language.orElse(language),
      filePath = other.filePath.orElse(filePath),
      sqlText = other.sqlText.orElse(sqlText),
      line = other.line.orElse(line),
      startPosition = other.startPosition.orElse(startPosition),
      objectType = other.objectType.orElse(objectType),
      objectName = other.objectName.orElse(objectName)
    )
  }

  /**
   * Merge values from the catalyst origin.
   *
   * The result has fields set to the value in the other origin if it is defined, or if not, then
   * the value in this origin.
   */
  def merge(other: Origin): QueryOrigin = {
    merge(
      QueryOrigin(
        sqlText = other.sqlText,
        line = other.line,
        startPosition = other.startPosition
      )
    )
  }
}

object QueryOrigin extends Logging {

  /** An empty QueryOrigin without any provenance information. */
  val empty: QueryOrigin = QueryOrigin()

  /**
   * An exception that wraps `QueryOrigin` and lets us store it in errors as suppressed
   * exceptions.
   */
  private case class QueryOriginWrapper(origin: QueryOrigin) extends Exception with NoStackTrace

  implicit class ExceptionHelpers(t: Throwable) {

    /**
     * Stores `origin` inside the given throwable using suppressed exceptions.
     *
     * We rely on suppressed exceptions since that lets us preserve the original exception class
     * and type.
     */
    def addOrigin(origin: QueryOrigin): Throwable = {
      // Only try to add the query context if one has not already been added.
      try {
        // Do not add the origin again if one is already present.
        // This also handles the case where the throwable is `null`.
        if (getOrigin(t).isEmpty) {
          t.addSuppressed(QueryOriginWrapper(origin))
        }
      } catch {
        case NonFatal(e) =>
          logError("Failed to add pipeline context", e)
      }
      t
    }
  }

  /** Returns the `QueryOrigin` stored as a suppressed exception in the given throwable.
   *
   * @return Some(origin) if the origin is recorded as part of the given throwable, `None`
   *         otherwise.
   */
  def getOrigin(t: Throwable): Option[QueryOrigin] = {
    try {
      // Wrap in an `Option(_)` first to handle `null` throwable.
      Option(t).flatMap { ex =>
        ex.getSuppressed.collectFirst {
          case QueryOriginWrapper(context) => context
        }
      }
    } catch {
      case NonFatal(e) =>
        logError("Failed to get pipeline context", e)
        None
    }
  }
}

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

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.{SQLFunction, SqlPathFormat, UserDefinedFunction}
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

/**
 * Helpers for [[DescribeFunctionCommand]] to retrieve and format
 * the frozen SQL PATH stored in SQL function metadata.
 */
private[command] object DescribeFunctionCommandUtils {

  /**
   * Returns the frozen SQL PATH persisted for a SQL function, formatted
   * for display. Persistent functions: loads [[CatalogFunction]] metadata
   * from the catalog. Temporary SQL UDFs (not in catalog): falls back to
   * parsing the usage JSON blob produced by [[SQLFunction.toExpressionInfo]].
   */
  private[command] def storedResolutionPathString(
      sparkSession: SparkSession,
      identifier: FunctionIdentifier,
      info: ExpressionInfo): Option[String] = {
    val rawJson = try {
      val meta = sparkSession.sessionState.catalog
        .getFunctionMetadata(identifier)
      if (meta.isUserDefinedFunction) {
        val udf = UserDefinedFunction.fromCatalogFunction(
          meta,
          sparkSession.sessionState.sqlParser)
        udf.asInstanceOf[SQLFunction].functionStoredResolutionPath
      } else {
        None
      }
    } catch {
      case _: org.apache.spark.sql.catalyst.analysis
        .NoSuchFunctionException |
          _: org.apache.spark.sql.catalyst.analysis
            .NoSuchDatabaseException =>
        extractResolutionPathFromSqlUdfUsage(info.getUsage)
    }
    rawJson.flatMap(formatStoredPath)
  }

  private def formatStoredPath(pathStr: String): Option[String] = {
    SqlPathFormat.toDescribeJson(pathStr)
      .flatMap(SqlPathFormat.formatForDisplay)
  }

  /**
   * For temporary SQL UDFs not in the catalog, the resolution path may
   * be embedded in the ExpressionInfo usage JSON blob. Returns None if
   * the usage string is not JSON or does not contain the path key.
   */
  private def extractResolutionPathFromSqlUdfUsage(
      usage: String): Option[String] = {
    if (usage == null || usage.isEmpty) return None
    try {
      val map = UserDefinedFunction.mapper.readValue(
        usage, classOf[util.HashMap[String, String]])
      Option(map.get(SQLFunction.FUNCTION_RESOLUTION_PATH))
        .filter(_.nonEmpty)
    } catch {
      case e: com.fasterxml.jackson.core.JsonProcessingException =>
        throw new org.apache.spark.SparkException(
          s"Corrupted SQL UDF metadata: expected JSON usage blob " +
          s"but failed to parse: ${e.getMessage}", e)
    }
  }
}

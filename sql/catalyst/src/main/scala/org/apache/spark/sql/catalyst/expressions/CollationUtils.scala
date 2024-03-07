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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.{DataType, StringType}

object CollationUtils {
  def checkCollationCompatibility(
                                   superCheck: => TypeCheckResult,
                                   collationId: Int,
                                   rightDataType: DataType
                                 ): TypeCheckResult = {
    val checkResult = superCheck
    if (checkResult.isFailure) return checkResult
    // Additional check needed for collation compatibility
    val rightCollationId: Int = rightDataType.asInstanceOf[StringType].collationId
    if (collationId != rightCollationId) {
      return DataTypeMismatch(
        errorSubClass = "COLLATION_MISMATCH",
        messageParameters = Map(
          "collationNameLeft" -> CollationFactory.fetchCollation(collationId).collationName,
          "collationNameRight" -> CollationFactory.fetchCollation(rightCollationId).collationName
        )
      )
    }
    TypeCheckResult.TypeCheckSuccess
  }

  final val SUPPORT_BINARY_ONLY: Int = 0
  final val SUPPORT_LOWERCASE: Int = 1
  final val SUPPORT_ALL_COLLATIONS: Int = 2

  def checkCollationSupport(
                             superCheck: => TypeCheckResult,
                             collationId: Int,
                             functionName: String,
                             supportLevel: Int = SUPPORT_BINARY_ONLY
                           ): TypeCheckResult = {
    val checkResult = superCheck
    if (checkResult.isFailure) return checkResult
    // Additional check needed for collation support
    val collation = CollationFactory.fetchCollation(collationId)
    supportLevel match {
      case SUPPORT_BINARY_ONLY =>
        if (!collation.isBinaryCollation) {
          throwUnsupportedCollation(functionName, collation.collationName)
        }
      case SUPPORT_LOWERCASE =>
        if (!collation.isBinaryCollation && !collation.isLowercaseCollation) {
          throwUnsupportedCollation(functionName, collation.collationName)
        }
      case SUPPORT_ALL_COLLATIONS => // No additional checks needed
      case _ => throw new IllegalArgumentException("Invalid collation support level.")
    }
    TypeCheckResult.TypeCheckSuccess
  }

  private def throwUnsupportedCollation(functionName: String, collationName: String): Unit = {
    throw new SparkException(
      errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
      messageParameters = Map(
        "functionName" -> functionName,
        "collationName" -> collationName),
      cause = null
    )
  }
}

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

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.types.{DataType, StringType}

object CollationTypeConstraints {
  def checkCollationCompatibilityAndSupport(
      checkResult: TypeCheckResult,
      collationId: Int,
      dataTypes: Seq[DataType],
      functionName: String,
      collationSupportLevel: CollationSupportLevel.CollationSupportLevel): TypeCheckResult = {
    val resultCompatibility = checkCollationCompatibility(checkResult, collationId, dataTypes)
    checkCollationSupport(resultCompatibility, collationId, functionName, collationSupportLevel)
  }

  def checkCollationCompatibility(
      checkResult: TypeCheckResult,
      collationId: Int,
      dataTypes: Seq[DataType]): TypeCheckResult = {
    if (checkResult.isFailure) {
      return checkResult
    }
    val collationName = CollationFactory.fetchCollation(collationId).collationName
    // Additional check needed for collation compatibility
    for (dataType <- dataTypes) {
      dataType match {
        case stringType: StringType =>
          if (stringType.collationId != collationId) {
            val collation = CollationFactory.fetchCollation(stringType.collationId)
            return DataTypeMismatch(
              errorSubClass = "COLLATION_MISMATCH",
              messageParameters = Map(
                "collationNameLeft" -> collationName,
                "collationNameRight" -> collation.collationName
              )
            )
          }
        case _ =>
      }
    }
    TypeCheckResult.TypeCheckSuccess
  }

  object CollationSupportLevel extends Enumeration {
    type CollationSupportLevel = Value

    val SUPPORT_BINARY_ONLY: Value = Value(0)
    val SUPPORT_LOWERCASE: Value = Value(1)
    val SUPPORT_ALL_COLLATIONS: Value = Value(2)
  }

  def checkCollationSupport(
      checkResult: TypeCheckResult,
      collationId: Int,
      functionName: String,
      collationSupportLevel: CollationSupportLevel.CollationSupportLevel)
  : TypeCheckResult = {
    if (checkResult.isFailure) {
      return checkResult
    }
    // Additional check needed for collation support
    val collation = CollationFactory.fetchCollation(collationId)
    collationSupportLevel match {
      case CollationSupportLevel.SUPPORT_BINARY_ONLY =>
        if (!collation.isBinaryCollation) {
          throwUnsupportedCollation(functionName, collation.collationName)
        }
      case CollationSupportLevel.SUPPORT_LOWERCASE =>
        if (!collation.isBinaryCollation && !collation.isLowercaseCollation) {
          throwUnsupportedCollation(functionName, collation.collationName)
        }
      case CollationSupportLevel.SUPPORT_ALL_COLLATIONS => // No additional checks needed
      case _ => throw new SparkIllegalArgumentException("Invalid collation support level.")
    }
    TypeCheckResult.TypeCheckSuccess
  }

  private def throwUnsupportedCollation(functionName: String, collationName: String): Unit = {
    throw new SparkException(
      errorClass = "UNSUPPORTED_COLLATION.FOR_FUNCTION",
      messageParameters = Map(
        "functionName" -> toSQLId(functionName),
        "collationName" -> collationName),
      cause = null
    )
  }
}

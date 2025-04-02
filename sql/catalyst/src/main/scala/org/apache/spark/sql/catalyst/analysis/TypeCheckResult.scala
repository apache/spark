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

/**
 * Represents the result of `Expression.checkInputDataTypes`.
 * We will throw `AnalysisException` in `CheckAnalysis` if `isFailure` is true.
 */
trait TypeCheckResult {
  def isFailure: Boolean = !isSuccess
  def isSuccess: Boolean
}

object TypeCheckResult {

  /**
   * Represents the successful result of `Expression.checkInputDataTypes`.
   */
  object TypeCheckSuccess extends TypeCheckResult {
    def isSuccess: Boolean = true
  }

  /**
   * Represents the failing result of `Expression.checkInputDataTypes`,
   * with an error message to show the reason of failure.
   */
  case class TypeCheckFailure(message: String) extends TypeCheckResult {
    def isSuccess: Boolean = false
  }

  /**
   * Represents an error of data type mismatch with the `DATATYPE_MISMATCH` error class.
   *
   * @param errorSubClass A sub-class of `DATATYPE_MISMATCH`.
   * @param messageParameters Parameters of the sub-class error message.
   */
  case class DataTypeMismatch(
      errorSubClass: String,
      messageParameters: Map[String, String] = Map.empty)
    extends TypeCheckResult {
    def isSuccess: Boolean = false
  }

  /**
   * Represents an error of invalid format with the `INVALID_FORMAT` error class.
   *
   * @param errorSubClass A sub-class of `INVALID_FORMAT`.
   * @param messageParameters Parameters of the sub-class error message.
   */
  case class InvalidFormat(
      errorSubClass: String,
      messageParameters: Map[String, String] = Map.empty)
    extends TypeCheckResult {
    def isSuccess: Boolean = false
  }
}

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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.{LanguageSQL, RoutineLanguage, UserDefinedFunctionErrors}
import org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData

/**
 * The base class for CreateUserDefinedFunctionCommand
 */
abstract class CreateUserDefinedFunctionCommand
  extends LeafRunnableCommand with IgnoreCachedData


object CreateUserDefinedFunctionCommand {

  /**
   * This factory methods serves as a central place to verify required inputs and
   * returns the CREATE command for the parsed user defined function.
   */
  // scalastyle:off argcount
  def apply(
      name: FunctionIdentifier,
      inputParamText: Option[String],
      returnTypeText: String,
      exprText: Option[String],
      queryText: Option[String],
      comment: Option[String],
      isDeterministic: Option[Boolean],
      containsSQL: Option[Boolean],
      language: RoutineLanguage,
      isTableFunc: Boolean,
      isTemp: Boolean,
      ignoreIfExists: Boolean,
      replace: Boolean
  ): CreateUserDefinedFunctionCommand = {
    // scalastyle:on argcount

    assert(language != null)

    language match {
      case LanguageSQL =>
        CreateSQLFunctionCommand(
          name,
          inputParamText,
          returnTypeText,
          exprText,
          queryText,
          comment,
          isDeterministic,
          containsSQL,
          isTableFunc,
          isTemp,
          ignoreIfExists,
          replace)

      case other =>
        throw UserDefinedFunctionErrors.unsupportedUserDefinedFunction(other)
    }
  }
}

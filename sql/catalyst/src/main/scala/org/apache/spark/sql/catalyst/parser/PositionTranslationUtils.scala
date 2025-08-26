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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.trees.SQLQueryContext

/**
 * Utility functions for translating error positions between substituted and original SQL text.
 *
 * This object provides common functionality used by both the parser (for parse-time errors)
 * and the execution engine (for runtime errors) to translate error positions from substituted
 * SQL back to the original SQL text that the user submitted.
 */
object PositionTranslationUtils {

  /**
   * Translate SQL query context positions from substituted text back to original text.
   *
   * When parameters are substituted in SQL text, error positions change. This method
   * uses a PositionMapper to translate error positions back to the original SQL text
   * that the user submitted, ensuring error messages show accurate positions.
   *
   * @param context The SQL query context with positions in substituted text
   * @param mapper The position mapper for translation
   * @return The context with positions translated to original text
   */
  def translateSqlContext(
      context: SQLQueryContext,
      mapper: PositionMapper): SQLQueryContext = {
    val translatedStartIndex = context.originStartIndex.map(mapper.mapToOriginal)
    val translatedStopIndex = context.originStopIndex.map(mapper.mapToOriginal)

    SQLQueryContext(
      line = context.line,
      startPosition = context.startPosition,
      originStartIndex = translatedStartIndex,
      originStopIndex = translatedStopIndex,
      sqlText = Some(mapper.originalText), // Use original text instead of substituted
      originObjectType = context.originObjectType,
      originObjectName = context.originObjectName
    )
  }
}

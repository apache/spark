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

package org.apache.spark.sql.catalyst.expressions.variant

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.types.variant.{VariantBuilder, VariantSizeLimitException, VariantUtil}
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

/**
 * A utility class for constructing variant expressions.
 */
object VariantExpressionEvalUtils {

  def parseJson(input: UTF8String): VariantVal = {
    try {
      val v = VariantBuilder.parseJson(input.toString)
      new VariantVal(v.getValue, v.getMetadata)
    } catch {
      case _: VariantSizeLimitException =>
        throw QueryExecutionErrors.variantSizeLimitError(VariantUtil.SIZE_LIMIT, "parse_json")
      case NonFatal(e) =>
        throw QueryExecutionErrors.malformedRecordsDetectedInRecordParsingError(
          input.toString, BadRecordException(() => input, cause = e))
    }
  }

  def isVariantNull(input: VariantVal): Boolean = {
    if (input == null) {
      // This is a SQL NULL, not a Variant NULL
      false
    } else {
      val variantValue = input.getValue
      // Variant NULL is denoted by basic_type == 0 and val_header == 0
      variantValue(0) == 0
    }
  }
}

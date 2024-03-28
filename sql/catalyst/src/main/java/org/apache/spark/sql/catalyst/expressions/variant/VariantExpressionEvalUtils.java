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

package org.apache.spark.sql.catalyst.expressions.variant;

import scala.util.control.NonFatal;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.BadRecordException;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantUtil;
import org.apache.spark.types.variant.VariantBuilder;
import org.apache.spark.types.variant.VariantSizeLimitException;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * A utility class for constructing variant expressions.
 */
public class VariantExpressionEvalUtils {

  public static VariantVal parseJson(UTF8String input) {
    try {
      Variant v = VariantBuilder.parseJson(input.toString());
      return new VariantVal(v.getValue(), v.getMetadata());
    } catch (VariantSizeLimitException e) {
      throw QueryExecutionErrors.variantSizeLimitError(VariantUtil.SIZE_LIMIT, "parse_json");
    } catch (Throwable throwable) {
      if (NonFatal.apply(throwable)) {
        throw QueryExecutionErrors.malformedRecordsDetectedInRecordParsingError(
            input.toString(),
            new BadRecordException(() -> input, () -> new InternalRow[0], throwable));
      }
      throw new Error(throwable);
    }
  }
}

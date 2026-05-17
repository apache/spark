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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.QueryContext;
import org.apache.spark.sql.errors.QueryExecutionErrors;

/**
 * Static helpers used by {@link ElementAt} on {@code ArrayType}
 * (codegen and eval) under ANSI mode.
 */
public final class ElementAtUtils {

  private ElementAtUtils() {}

  /**
   * Resolves the user-supplied 1-based {@code element_at} index to a
   * 0-based array position. Throws when the absolute index exceeds the
   * array length (ANSI out-of-bounds) or when {@code index} is zero
   * (always invalid).
   *
   * @param length  the array length
   * @param index   the 1-based index supplied by the user (positive or negative)
   * @param context the query context attached to the error
   * @return        the resolved 0-based position
   */
  public static int resolveArrayIndex(int length, int index, QueryContext context) {
    if (length < Math.abs(index)) {
      throw QueryExecutionErrors.invalidElementAtIndexError(index, length, context);
    }
    if (index == 0) {
      throw QueryExecutionErrors.invalidIndexOfZeroError(context);
    }
    return index > 0 ? index - 1 : length + index;
  }
}

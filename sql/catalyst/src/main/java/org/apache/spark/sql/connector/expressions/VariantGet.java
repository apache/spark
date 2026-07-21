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

package org.apache.spark.sql.connector.expressions;

import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.internal.connector.ExpressionWithToString;
import org.apache.spark.sql.types.DataType;

/**
 * A DSv2 connector expression that extracts a value from a variant column. It reads the value at
 * the JSON {@code path} from the variant {@code child} and casts it to {@code targetType}. This is
 * the connector-facing form of the {@code variant_get} / {@code try_variant_get} SQL functions:
 * {@link #failOnError()} selects between them (throw vs. return null on a cast failure), and
 * {@link #timeZoneId()} binds the time zone used for timestamp casts. {@code V2ExpressionBuilder}
 * produces it so data sources can push variant extractions down (for example into filters).
 *
 * @since 4.3.0
 */
@Evolving
public class VariantGet extends ExpressionWithToString {
  private final Expression child;
  private final String path;
  private final DataType targetType;
  private final boolean failOnError;
  private final String timeZoneId;

  /**
   * Creates VariantGet expression.
   * @param child variant column reference
   * @param path JSON path string
   * @param targetType expected result type
   * @param failOnError whether to throw on cast failure ({@code variant_get}) or return null
   *                    ({@code try_variant_get})
   * @param timeZoneId timezone bound on the catalyst expression for timestamp casts, or null
   */
  public VariantGet(
      Expression child,
      String path,
      DataType targetType,
      boolean failOnError,
      String timeZoneId) {
    this.child = child;
    this.path = path;
    this.targetType = targetType;
    this.failOnError = failOnError;
    this.timeZoneId = timeZoneId;
  }

  public Expression child() { return child; }
  public String path() { return path; }
  public DataType targetType() { return targetType; }
  public boolean failOnError() { return failOnError; }
  public String timeZoneId() { return timeZoneId; }

  @Override
  public Expression[] children() { return new Expression[]{ child }; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariantGet that = (VariantGet) o;
    return failOnError == that.failOnError &&
        Objects.equals(child, that.child) &&
        Objects.equals(path, that.path) &&
        Objects.equals(targetType, that.targetType) &&
        Objects.equals(timeZoneId, that.timeZoneId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(child, path, targetType, failOnError, timeZoneId);
  }
}

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
 * Represents a cast expression in the public logical expression API.
 *
 * @since 3.3.0
 */
@Evolving
public class Cast extends ExpressionWithToString {
  private Expression expression;

  /**
   * Original data type of given expression
   */
  private DataType expressionDataType;

  /**
   * Target data type, i.e. data type in which expression will be cast
   */
  private DataType dataType;

  @Deprecated
  public Cast(Expression expression, DataType dataType) {
    this(expression, null, dataType);
  }

  public Cast(Expression expression, DataType expressionDataType, DataType targetDataType) {
    this.expression = expression;
    this.expressionDataType = expressionDataType;
    this.dataType = targetDataType;
  }

  public Expression expression() { return expression; }
  public DataType expressionDataType() { return expressionDataType; }
  public DataType dataType() { return dataType; }

  @Override
  public Expression[] children() { return new Expression[]{ expression() }; }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    Cast that = (Cast) other;
    return Objects.equals(expression, that.expression) &&
        Objects.equals(expressionDataType, that.expressionDataType) &&
        Objects.equals(dataType, that.dataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, expressionDataType, dataType);
  }
}

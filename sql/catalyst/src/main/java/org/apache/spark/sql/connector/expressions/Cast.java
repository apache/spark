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
  private DataType dataType;

  public Cast(Expression expression, DataType dataType) {
    this.expression = expression;
    this.dataType = dataType;
  }

  public Expression expression() { return expression; }
  public DataType dataType() { return dataType; }

  @Override
  public Expression[] children() { return new Expression[]{ expression() }; }
}

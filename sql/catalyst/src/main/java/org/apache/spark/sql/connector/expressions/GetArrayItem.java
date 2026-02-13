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

/**
 * Get array item expression.
 *
 * @since 4.1.0
 */

@Evolving
public class GetArrayItem extends ExpressionWithToString {

  private final Expression childArray;
  private final Expression ordinal;
  private final boolean failOnError;

  /**
   * Creates GetArrayItem expression.
   * @param childArray Array that is source to get element from. Child of this expression.
   * @param ordinal Ordinal of element. Zero-based indexing.
   * @param failOnError Whether expression should throw exception for index out of bound or to
   *                    return null.
   */
  public GetArrayItem(Expression childArray, Expression ordinal, boolean failOnError) {
    this.childArray = childArray;
    this.ordinal = ordinal;
    this.failOnError = failOnError;
  }

  public Expression childArray() { return this.childArray; }
  public Expression ordinal() { return this.ordinal; }
  public boolean failOnError() { return this.failOnError; }

  @Override
  public Expression[] children() { return new Expression[]{ childArray, ordinal }; }
}

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
 * Represent an extract function, which extracts and returns the value of a
 * specified datetime field from a datetime or interval value expression.
 * <p>
 * The currently supported fields names following the ISO standard:
 * <ol>
 *  <li> <code>SECOND</code> Since 3.4.0 </li>
 *  <li> <code>MINUTE</code> Since 3.4.0 </li>
 *  <li> <code>HOUR</code> Since 3.4.0 </li>
 *  <li> <code>MONTH</code> Since 3.4.0 </li>
 *  <li> <code>QUARTER</code> Since 3.4.0 </li>
 *  <li> <code>YEAR</code> Since 3.4.0 </li>
 *  <li> <code>DAY_OF_WEEK</code> Since 3.4.0 </li>
 *  <li> <code>DAY</code> Since 3.4.0 </li>
 *  <li> <code>DAY_OF_YEAR</code> Since 3.4.0 </li>
 *  <li> <code>WEEK</code> Since 3.4.0 </li>
 *  <li> <code>YEAR_OF_WEEK</code> Since 3.4.0 </li>
 * </ol>
 *
 * @since 3.4.0
 */

@Evolving
public class Extract extends ExpressionWithToString {

  private String field;
  private Expression source;

  public Extract(String field, Expression source) {
    this.field = field;
    this.source = source;
  }

  public String field() { return field; }
  public Expression source() { return source; }

  @Override
  public Expression[] children() { return new Expression[]{ source() }; }
}

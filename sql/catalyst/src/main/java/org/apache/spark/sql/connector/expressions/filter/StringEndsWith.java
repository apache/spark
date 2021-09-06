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

package org.apache.spark.sql.connector.expressions.filter;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A filter that evaluates to `true` iff the field evaluates to
 * a string that ends with `value`.
 *
 * @since 3.3.0
 */
@Evolving
public final class StringEndsWith extends Filter {
  private final FieldReference expr;
  private final String value;

  public StringEndsWith( FieldReference expr, String value) {
    this.expr = expr;
    this.value = value;
  }

  public  FieldReference expr() { return expr; }
  public String value() { return value; }

  @Override
  public String toString() { return "STRING_ENDS_WITH(" + expr.describe() + ", " + value + ")"; }

  @Override
  public NamedReference[] references() { return expr.references(); }
}

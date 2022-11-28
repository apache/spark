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

import java.util.Arrays;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.internal.connector.ExpressionWithToString;

/**
 * The general representation of user defined scalar function, which contains the upper-cased
 * function name, canonical function name and all the children expressions.
 *
 * @since 3.4.0
 */
@Evolving
public class UserDefinedScalarFunc extends ExpressionWithToString {
  private String name;
  private String canonicalName;
  private Expression[] children;

  public UserDefinedScalarFunc(String name, String canonicalName, Expression[] children) {
    this.name = name;
    this.canonicalName = canonicalName;
    this.children = children;
  }

  public String name() { return name; }
  public String canonicalName() { return canonicalName; }

  @Override
  public Expression[] children() { return children; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UserDefinedScalarFunc that = (UserDefinedScalarFunc) o;
    return Objects.equals(name, that.name) && Objects.equals(canonicalName, that.canonicalName) &&
      Arrays.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, canonicalName, children);
  }
}

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

package org.apache.spark.sql.connector.expressions.aggregate;

import java.util.Arrays;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.internal.connector.ExpressionWithToString;

/**
 * The general representation of user defined aggregate function, which implements
 * {@link AggregateFunc}, contains the upper-cased function name, the canonical function name,
 * the `isDistinct` flag and all the inputs. Note that Spark cannot push down aggregate with
 * this function partially to the source, but can only push down the entire aggregate.
 *
 * @since 3.4.0
 */
@Evolving
public class UserDefinedAggregateFunc extends ExpressionWithToString implements AggregateFunc {
  private final String name;
  private String canonicalName;
  private final boolean isDistinct;
  private final Expression[] children;

  public UserDefinedAggregateFunc(
      String name, String canonicalName, boolean isDistinct, Expression[] children) {
    this.name = name;
    this.canonicalName = canonicalName;
    this.isDistinct = isDistinct;
    this.children = children;
  }

  public String name() { return name; }
  public String canonicalName() { return canonicalName; }
  public boolean isDistinct() { return isDistinct; }

  @Override
  public Expression[] children() { return children; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    UserDefinedAggregateFunc that = (UserDefinedAggregateFunc) o;

    if (isDistinct != that.isDistinct) return false;
    if (!name.equals(that.name)) return false;
    if (!canonicalName.equals(that.canonicalName)) return false;
    return Arrays.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + canonicalName.hashCode();
    result = 31 * result + (isDistinct ? 1 : 0);
    result = 31 * result + Arrays.hashCode(children);
    return result;
  }
}

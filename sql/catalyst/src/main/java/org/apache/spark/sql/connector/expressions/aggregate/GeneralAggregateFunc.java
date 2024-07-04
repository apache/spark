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
import org.apache.spark.sql.connector.expressions.SortValue;
import org.apache.spark.sql.internal.connector.ExpressionWithToString;

/**
 * The general implementation of {@link AggregateFunc}, which contains the upper-cased function
 * name, the `isDistinct` flag and all the inputs. Note that Spark cannot push down partial
 * aggregate with this function to the source, but can only push down the entire aggregate.
 * <p>
 * The currently supported SQL aggregate functions:
 * <ol>
 *  <li><pre>VAR_POP(input1)</pre> Since 3.3.0</li>
 *  <li><pre>VAR_SAMP(input1)</pre> Since 3.3.0</li>
 *  <li><pre>STDDEV_POP(input1)</pre> Since 3.3.0</li>
 *  <li><pre>STDDEV_SAMP(input1)</pre> Since 3.3.0</li>
 *  <li><pre>COVAR_POP(input1, input2)</pre> Since 3.3.0</li>
 *  <li><pre>COVAR_SAMP(input1, input2)</pre> Since 3.3.0</li>
 *  <li><pre>CORR(input1, input2)</pre> Since 3.3.0</li>
 *  <li><pre>REGR_INTERCEPT(input1, input2)</pre> Since 3.4.0</li>
 *  <li><pre>REGR_R2(input1, input2)</pre> Since 3.4.0</li>
 *  <li><pre>REGR_SLOPE(input1, input2)</pre> Since 3.4.0</li>
 *  <li><pre>REGR_SXY(input1, input2)</pre> Since 3.4.0</li>
 *  <li><pre>MODE() WITHIN (ORDER BY input1 [ASC|DESC])</pre> Since 4.0.0</li>
 *  <li><pre>PERCENTILE_CONT(input1) WITHIN (ORDER BY input2 [ASC|DESC])</pre> Since 4.0.0</li>
 *  <li><pre>PERCENTILE_DISC(input1) WITHIN (ORDER BY input2 [ASC|DESC])</pre> Since 4.0.0</li>
 * </ol>
 *
 * @since 3.3.0
 */
@Evolving
public final class GeneralAggregateFunc extends ExpressionWithToString implements AggregateFunc {
  private final String name;
  private final boolean isDistinct;
  private final Expression[] children;
  private final SortValue[] orderingWithinGroups;

  public GeneralAggregateFunc(String name, boolean isDistinct, Expression[] children) {
    this.name = name;
    this.isDistinct = isDistinct;
    this.children = children;
    this.orderingWithinGroups = new SortValue[]{};
  }

  public GeneralAggregateFunc(
      String name, boolean isDistinct, Expression[] children, SortValue[] orderingWithinGroups) {
    this.name = name;
    this.isDistinct = isDistinct;
    this.children = children;
    this.orderingWithinGroups = orderingWithinGroups;
  }

  public String name() { return name; }
  public boolean isDistinct() { return isDistinct; }

  @Override
  public Expression[] children() { return children; }

  public SortValue[] orderingWithinGroups() { return orderingWithinGroups; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GeneralAggregateFunc that = (GeneralAggregateFunc) o;

    if (isDistinct != that.isDistinct) return false;
    if (!name.equals(that.name)) return false;
    if (!Arrays.equals(children, that.children)) return false;
    return Arrays.equals(orderingWithinGroups, that.orderingWithinGroups);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + (isDistinct ? 1 : 0);
    result = 31 * result + Arrays.hashCode(children);
    result = 31 * result + Arrays.hashCode(orderingWithinGroups);
    return result;
  }
}

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
import java.util.stream.Collectors;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * The general implementation of {@link AggregateFunc}, which contains the upper-cased function
 * name, the `isDistinct` flag and all the inputs. Note that Spark cannot push down partial
 * aggregate with this function to the source, but can only push down the entire aggregate.
 * <p>
 * The currently supported SQL aggregate functions:
 * <ol>
 *  <li><pre>AVG(input1)</pre> Since 3.3.0</li>
 * </ol>
 *
 * @since 3.3.0
 */
@Evolving
public final class GeneralAggregateFunc implements AggregateFunc {
  private final String name;
  private final boolean isDistinct;
  private final NamedReference[] inputs;

  public String name() { return name; }
  public boolean isDistinct() { return isDistinct; }
  public NamedReference[] inputs() { return inputs; }

  public GeneralAggregateFunc(String name, boolean isDistinct, NamedReference[] inputs) {
    this.name = name;
    this.isDistinct = isDistinct;
    this.inputs = inputs;
  }

  @Override
  public String toString() {
    String inputsString = Arrays.stream(inputs)
      .map(Expression::describe)
      .collect(Collectors.joining(", "));
    if (isDistinct) {
      return name + "(DISTINCT " + inputsString + ")";
    } else {
      return name + "(" + inputsString + ")";
    }
  }
}

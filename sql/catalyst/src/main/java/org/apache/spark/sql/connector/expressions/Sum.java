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
import org.apache.spark.sql.types.DataType;

/**
 * An aggregate function that returns the summation of all the values in a group.
 *
 * @since 3.2.0
 */
@Evolving
public final class Sum implements AggregateFunc {
  private final FieldReference column;
  private final DataType dataType;
  private final boolean isDistinct;

  public Sum(FieldReference column, DataType dataType, boolean isDistinct) {
    this.column = column;
    this.dataType = dataType;
    this.isDistinct = isDistinct;
  }

  public FieldReference column() { return column; }
  public DataType dataType() { return dataType; }
  public boolean isDistinct() { return isDistinct; }

  @Override
  public String toString() {
    return "Sum(" + column.describe() + "," + dataType + "," + isDistinct + ")";
  }

  @Override
  public String describe() { return this.toString(); }
}

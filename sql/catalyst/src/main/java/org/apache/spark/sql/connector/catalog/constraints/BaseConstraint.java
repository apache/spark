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

package org.apache.spark.sql.connector.catalog.constraints;

import org.apache.spark.sql.connector.expressions.NamedReference;

import java.util.StringJoiner;

abstract class BaseConstraint implements Constraint {

  private final String name;
  private final ConstraintState state;

  protected BaseConstraint(String name, ConstraintState state) {
    this.name = name;
    this.state = state;
  }

  protected abstract String definition();

  @Override
  public String name() {
    return name;
  }

  @Override
  public ConstraintState state() {
    return state;
  }

  @Override
  public String toDDL() {
    return String.format(
        "CONSTRAINT %s %s %s %s %s",
        name,
        definition(),
        state.enforced() ? "ENFORCED" : "NOT ENFORCED",
        state.validated() ? "VALID" : "NOT VALID",
        state.rely() ? "RELY" : "NORELY");
  }

  @Override
  public String toString() {
    return toDDL();
  }

  protected String toSQL(NamedReference[] columns) {
    StringJoiner joiner = new StringJoiner(", ");

    for (NamedReference column : columns) {
      joiner.add(column.toString());
    }

    return joiner.toString();
  }
}

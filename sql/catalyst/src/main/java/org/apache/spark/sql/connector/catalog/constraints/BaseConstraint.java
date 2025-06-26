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

import java.util.StringJoiner;

import org.apache.spark.sql.connector.expressions.NamedReference;

abstract class BaseConstraint implements Constraint {

  private final String name;
  private final boolean enforced;
  private final ValidationStatus validationStatus;
  private final boolean rely;

  protected BaseConstraint(
      String name,
      boolean enforced,
      ValidationStatus validationStatus,
      boolean rely) {
    this.name = name;
    this.enforced = enforced;
    this.validationStatus = validationStatus;
    this.rely = rely;
  }

  protected abstract String definition();

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean enforced() {
    return enforced;
  }

  @Override
  public ValidationStatus validationStatus() {
    return validationStatus;
  }

  @Override
  public boolean rely() {
    return rely;
  }

  @Override
  public String toDDL() {
    return String.format(
        "CONSTRAINT %s %s %s %s %s",
        name,
        definition(),
        enforced ? "ENFORCED" : "NOT ENFORCED",
        validationStatus,
        rely ? "RELY" : "NORELY");
  }

  @Override
  public String toString() {
    return toDDL();
  }

  protected String toDDL(NamedReference[] columns) {
    StringJoiner joiner = new StringJoiner(", ");

    for (NamedReference column : columns) {
      joiner.add(column.toString());
    }

    return joiner.toString();
  }

  abstract static class Builder<B, C> {
    private final String name;
    private boolean enforced = true;
    private ValidationStatus validationStatus = ValidationStatus.UNVALIDATED;
    private boolean rely = false;

    Builder(String name) {
      this.name = name;
    }

    protected abstract B self();

    public abstract C build();

    public String name() {
      return name;
    }

    public B enforced(boolean enforced) {
      this.enforced = enforced;
      return self();
    }

    public boolean enforced() {
      return enforced;
    }

    public B validationStatus(ValidationStatus validationStatus) {
      if (validationStatus != null) {
        this.validationStatus = validationStatus;
      }
      return self();
    }

    public ValidationStatus validationStatus() {
      return validationStatus;
    }

    public B rely(boolean rely) {
      this.rely = rely;
      return self();
    }

    public boolean rely() {
      return rely;
    }
  }
}

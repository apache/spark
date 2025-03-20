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

import java.util.Map;
import java.util.Objects;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

/**
 * A CHECK constraint.
 * <p>
 * A CHECK constraint defines a condition each row in a table must satisfy. Connectors can define
 * such constraints either in SQL (Spark SQL dialect) or using a {@link Predicate predicate} if the
 * condition can be expressed using a supported expression. A CHECK constraint can reference one or
 * more columns. Such constraint is considered violated if its condition evaluates to {@code FALSE},
 * but not {@code NULL}. The search condition must be deterministic and cannot contain subqueries
 * and certain functions like aggregates or UDFs.
 *
 * @since 4.1.0
 */
public class Check extends BaseConstraint {

  private final String sql;
  private final Predicate predicate;

  private Check(
      String name,
      String sql,
      Predicate predicate,
      boolean enforced,
      ValidationStatus validationStatus,
      boolean rely) {
    super(name, enforced, validationStatus, rely);
    this.sql = sql;
    this.predicate = predicate;
  }

  /**
   * Returns the SQL representation of the search condition (Spark SQL dialect).
   */
  public String sql() {
    return sql;
  }

  /**
   * Returns the search condition.
   */
  public Predicate predicate() {
    return predicate;
  }

  @Override
  protected String definition() {
    return String.format("CHECK %s", sql != null ? sql : predicate);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    Check that = (Check) other;
    return Objects.equals(name(), that.name()) &&
        Objects.equals(sql, that.sql) &&
        Objects.equals(predicate, that.predicate) &&
        enforced() == that.enforced() &&
        Objects.equals(validationStatus(), that.validationStatus()) &&
        rely() == that.rely();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name(), sql, predicate, enforced(), validationStatus(), rely());
  }

  public static class Builder extends BaseConstraint.Builder<Builder, Check> {

    private String sql;
    private Predicate predicate;

    Builder(String name) {
      super(name);
    }

    @Override
    protected Builder self() {
      return this;
    }

    public Builder sql(String sql) {
      this.sql = sql;
      return this;
    }

    public Builder predicate(Predicate predicate) {
      this.predicate = predicate;
      return this;
    }

    public Check build() {
      if (sql == null && predicate == null) {
        throw new SparkIllegalArgumentException(
            "INTERNAL_ERROR",
            Map.of("message", "SQL and predicate in CHECK can't be both null"));
      }
      return new Check(name(), sql, predicate, enforced(), validationStatus(), rely());
    }
  }
}

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
import org.apache.spark.annotation.Evolving;
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
 * <p>
 * Spark supports enforced and not enforced CHECK constraints, allowing connectors to control
 * whether data modifications that violate the constraint must fail. Each constraint is either
 * valid (the existing data is guaranteed to satisfy the constraint), invalid (some records violate
 * the constraint), or unvalidated (the validity is unknown). If the validity is unknown, Spark
 * will check {@link #rely()} to see whether the constraint is believed to be true and can be used
 * for query optimization.
 *
 * @since 4.1.0
 */
@Evolving
public class Check extends BaseConstraint {

  private final String predicateSql;
  private final Predicate predicate;

  private Check(
      String name,
      String predicateSql,
      Predicate predicate,
      boolean enforced,
      ValidationStatus validationStatus,
      boolean rely) {
    super(name, enforced, validationStatus, rely);
    this.predicateSql = predicateSql;
    this.predicate = predicate;
  }

  /**
   * Returns the SQL representation of the search condition (Spark SQL dialect).
   */
  public String predicateSql() {
    return predicateSql;
  }

  /**
   * Returns the search condition.
   */
  public Predicate predicate() {
    return predicate;
  }

  @Override
  protected String definition() {
    return String.format("CHECK (%s)", predicateSql != null ? predicateSql : predicate);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    Check that = (Check) other;
    return Objects.equals(name(), that.name()) &&
        Objects.equals(predicateSql, that.predicateSql) &&
        Objects.equals(predicate, that.predicate) &&
        enforced() == that.enforced() &&
        Objects.equals(validationStatus(), that.validationStatus()) &&
        rely() == that.rely();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name(), predicateSql, predicate, enforced(), validationStatus(), rely());
  }

  public static class Builder extends BaseConstraint.Builder<Builder, Check> {

    private String predicateSql;
    private Predicate predicate;

    Builder(String name) {
      super(name);
    }

    @Override
    protected Builder self() {
      return this;
    }

    public Builder predicateSql(String predicateSql) {
      this.predicateSql = predicateSql;
      return this;
    }

    public Builder predicate(Predicate predicate) {
      this.predicate = predicate;
      return this;
    }

    public Check build() {
      if (predicateSql == null && predicate == null) {
        throw new SparkIllegalArgumentException(
            "INTERNAL_ERROR",
            Map.of("message", "Predicate SQL and expression can't be both null in CHECK"));
      }
      return new Check(name(), predicateSql, predicate, enforced(), validationStatus(), rely());
    }
  }
}

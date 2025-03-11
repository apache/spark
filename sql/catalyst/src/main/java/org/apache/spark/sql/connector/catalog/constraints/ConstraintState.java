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

import java.util.Objects;

/**
 * A constraint state with the following properties:
 * <ul>
 *   <li><strong>Enforced:</strong> Indicates whether the constraint is actively enforced. If
 *   enforced, data modification operations that violate the constraint fail with a constraint
 *   violation error.</li>
 *
 *   <li><strong>Validated:</strong> Indicates whether the existing data in the table satisfies
 *   the constraint. A constraint may be validated independently from enforcement, meaning it can
 *   be validated without being actively enforced, or vice versa.</li>
 *
 *   <li><strong>Rely:</strong> Indicates whether the constraint is assumed to hold true even if it
 *   is not validated. The reliance state allows query optimizers to utilize the constraint for
 *   optimization purposes.</li>
 * </ul>
 *
 * @since 4.1.0
 */
public class ConstraintState {

  private final boolean enforced;
  private final boolean validated;
  private final boolean rely;

  private ConstraintState(boolean enforced, boolean validated, boolean rely) {
    this.enforced = enforced;
    this.validated = validated;
    this.rely = rely;
  }

  /**
   * Indicates whether the constraint is actively enforced.
   */
  public boolean enforced() {
    return enforced;
  }

  /**
   * Indicates whether the existing data is known to satisfy the constraint.
   */
  public boolean validated() {
    return validated;
  }

  /**
   * Indicates whether the constraint is assumed to hold true.
   */
  public boolean rely() {
    return rely;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    ConstraintState that = (ConstraintState) other;
    return enforced == that.enforced && validated == that.validated && rely == that.rely;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enforced, validated, rely);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private boolean enforced;
    private boolean validated;
    private boolean rely;

    private Builder() {}

    public Builder enforced(boolean value) {
      this.enforced = value;
      return this;
    }

    public Builder validated(boolean value) {
      this.validated = value;
      return this;
    }

    public Builder rely(boolean value) {
      this.rely = value;
      return this;
    }

    public ConstraintState build() {
      return new ConstraintState(enforced, validated, rely);
    }
  }
}

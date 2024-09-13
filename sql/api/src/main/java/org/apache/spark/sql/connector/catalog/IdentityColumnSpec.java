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

package org.apache.spark.sql.connector.catalog;
import java.util.Objects;

/**
 * Identity column specification.
 */
public class IdentityColumnSpec {
    private final long start;
    private final long step;
    private final boolean allowExplicitInsert;

    /**
     * Creates an identity column specification.
     * @param start the start value to generate the identity values
     * @param step the step value to generate the identity values
     * @param allowExplicitInsert whether the identity column allows explicit insertion of values
     */
    public IdentityColumnSpec(long start, long step, boolean allowExplicitInsert) {
      this.start = start;
      this.step = step;
      this.allowExplicitInsert = allowExplicitInsert;
    }

    /**
     * @return the start value to generate the identity values
     */
    public long getStart() {
      return start;
    }

    /**
     * @return the step value to generate the identity values
     */
    public long getStep() {
      return step;
    }

    /**
     * @return whether the identity column allows explicit insertion of values
     */
    public boolean isAllowExplicitInsert() {
      return allowExplicitInsert;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IdentityColumnSpec that = (IdentityColumnSpec) o;
      return start == that.start &&
              step == that.step &&
              allowExplicitInsert == that.allowExplicitInsert;
    }

    @Override
    public int hashCode() {
      return Objects.hash(start, step, allowExplicitInsert);
    }

    @Override
    public String toString() {
      return "IdentityColumnSpec{" +
              "start=" + start +
              ", step=" + step +
              ", allowExplicitInsert=" + allowExplicitInsert +
              "}";
    }
}

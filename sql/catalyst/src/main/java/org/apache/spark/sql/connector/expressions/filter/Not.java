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

package org.apache.spark.sql.connector.expressions.filter;

import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A filter that evaluates to {@code true} iff {@code child} is evaluated to {@code false}.
 *
 * @since 3.3.0
 */
@Evolving
public final class Not extends Filter {
  private final Filter child;

  public Not(Filter child) { this.child = child; }

  public Filter child() { return child; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Not not = (Not) o;
    return Objects.equals(child, not.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(child);
  }

  @Override
  public String toString() { return "NOT (" + child.describe() + ")"; }

  @Override
  public NamedReference[] references() { return child.references(); }
}

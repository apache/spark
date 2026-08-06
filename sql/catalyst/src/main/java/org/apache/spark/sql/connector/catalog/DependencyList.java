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

import java.util.Arrays;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;

/**
 * A list of dependencies for a SQL object such as a view or metric view.
 * <p>
 * <ul>
 *   <li>When {@code null}, the dependency information is not provided.</li>
 *   <li>When the array is empty, dependencies are provided but the object has none.</li>
 *   <li>When the array is non-empty, each entry describes one dependency.</li>
 * </ul>
 * <p>
 * Records' auto-generated {@code equals}/{@code hashCode} on array fields fall through to
 * {@link Object#equals} (reference equality), so this record overrides them to use
 * {@link Arrays#equals(Object[], Object[])} / {@link Arrays#hashCode(Object[])} on
 * {@code dependencies}; per-element equality delegates to the element's overridden
 * {@code equals} ({@link TableDependency} / {@link FunctionDependency} both implement value
 * semantics on their {@code nameParts} array). The defensive-copy accessor override clones
 * on read so callers cannot mutate the record's internal array.
 *
 * @param dependencies array of dependencies; must contain no null elements (defensive
 *                     copy made; not validated element-wise -- callers passing nulls will
 *                     surface NPEs in downstream consumers)
 * @since 4.2.0
 */
@Evolving
public record DependencyList(Dependency[] dependencies) {

  public DependencyList {
    Objects.requireNonNull(dependencies, "dependencies must not be null");
    dependencies = dependencies.clone();
  }

  /** Returns a defensive copy of the underlying dependencies array. */
  @Override
  public Dependency[] dependencies() { return dependencies.clone(); }

  @Override
  public boolean equals(Object o) {
    return o instanceof DependencyList that && Arrays.equals(dependencies, that.dependencies);
  }

  @Override
  public int hashCode() { return Arrays.hashCode(dependencies); }

  @Override
  public String toString() {
    return "DependencyList[dependencies=" + Arrays.toString(dependencies) + "]";
  }

  public static DependencyList of(Dependency[] dependencies) {
    return new DependencyList(dependencies);
  }
}

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

import java.util.List;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;

/**
 * A list of dependencies for a SQL object such as a view or metric view.
 * <p>
 * <ul>
 *   <li>When {@code null}, the dependency information is not provided.</li>
 *   <li>When the list is empty, dependencies are provided but the object has none.</li>
 *   <li>When the list is non-empty, each entry describes one dependency.</li>
 * </ul>
 * <p>
 * {@code dependencies} is held as an immutable {@link List} so the record's auto-generated
 * {@code equals}/{@code hashCode} delegate to per-element value semantics on the contained
 * {@link Dependency} entries.
 *
 * @param dependencies list of dependencies (immutable copy made)
 * @since 4.2.0
 */
@Evolving
public record DependencyList(List<Dependency> dependencies) {

  public DependencyList {
    Objects.requireNonNull(dependencies, "dependencies must not be null");
    dependencies = List.copyOf(dependencies);
  }

  public static DependencyList of(List<Dependency> dependencies) {
    return new DependencyList(dependencies);
  }
}

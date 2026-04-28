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

import org.apache.spark.annotation.Evolving;

/**
 * A list of dependencies for a SQL object such as a view or metric view.
 * <p>
 * <ul>
 *   <li>When {@code null}, the dependency information is not provided.</li>
 *   <li>When the array is empty, dependencies are provided but the object has none.</li>
 *   <li>When the array is non-empty, each entry describes one dependency.</li>
 * </ul>
 *
 * @param dependencies array of dependencies
 * @since 4.2.0
 */
@Evolving
public record DependencyList(Dependency[] dependencies) {

  public DependencyList {
    Objects.requireNonNull(dependencies, "dependencies must not be null");
  }

  public static DependencyList of(Dependency... dependencies) {
    return new DependencyList(dependencies);
  }
}

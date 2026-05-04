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
 * A function dependency of a SQL object.
 * <p>
 * The dependent function is identified by its structural multi-part name. See
 * {@link TableDependency} for the parts-form contract.
 * <p>
 * {@code nameParts} is held as an immutable {@link List} so the record's auto-generated
 * {@code equals}/{@code hashCode} delegate to per-element value semantics.
 *
 * @param nameParts structural multi-part identifier (immutable copy made; never empty)
 * @since 4.2.0
 */
@Evolving
public record FunctionDependency(List<String> nameParts) implements Dependency {
  public FunctionDependency {
    Objects.requireNonNull(nameParts, "nameParts must not be null");
    if (nameParts.isEmpty()) {
      throw new IllegalArgumentException("nameParts must not be empty");
    }
    nameParts = List.copyOf(nameParts);
  }
}

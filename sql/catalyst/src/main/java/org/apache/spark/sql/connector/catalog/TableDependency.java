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
 * A table dependency of a SQL object.
 * <p>
 * The dependent table is identified by its structural multi-part name. {@code nameParts}
 * arity matches the catalog's namespace depth plus one for the table name -- for a catalog
 * with single-level namespaces the parts are {@code [catalog, schema, table]}; for a catalog
 * with multi-level namespaces (e.g. Iceberg with {@code db1.db2}) the parts are
 * {@code [catalog, db1, db2, ..., table]}; for v1 sources resolved through the session
 * catalog, producers should normalize to {@code [spark_catalog, db, table]} so consumers see
 * a stable arity per source kind. The structural form preserves arity and is unambiguous
 * against quoted identifiers containing a literal {@code .}; consumers that need a flat
 * string should join the parts themselves with a quoting scheme appropriate to their wire
 * format.
 * <p>
 * {@code nameParts} is held as an immutable {@link List} so the record's auto-generated
 * {@code equals}/{@code hashCode} delegate to per-element value semantics.
 *
 * @param nameParts structural multi-part identifier (immutable copy made; never empty)
 * @since 4.2.0
 */
@Evolving
public record TableDependency(List<String> nameParts) implements Dependency {
  public TableDependency {
    Objects.requireNonNull(nameParts, "nameParts must not be null");
    if (nameParts.isEmpty()) {
      throw new IllegalArgumentException("nameParts must not be empty");
    }
    nameParts = List.copyOf(nameParts);
  }
}

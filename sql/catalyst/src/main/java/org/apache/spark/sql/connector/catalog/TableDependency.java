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
 * A table dependency of a SQL object.
 * <p>
 * The dependent table is identified by its structural multi-part name. {@code nameParts}
 * arity matches the catalog's namespace depth plus one for the table name -- for a catalog
 * with single-level namespaces the parts are typically
 * {@code [catalog, schema, table]}; for a catalog with multi-level namespaces (e.g. Iceberg
 * with {@code db1.db2}) the parts are {@code [catalog, db1, db2, ..., table]}; for sources
 * referenced through a session catalog without an explicit catalog component the parts can
 * be {@code [db, table]} or just {@code [table]}. The structural form preserves arity and
 * is unambiguous against quoted identifiers containing a literal {@code .}; consumers that
 * need a flat string should join the parts themselves with a quoting scheme appropriate to
 * their wire format.
 *
 * @param nameParts structural multi-part identifier (defensive copy made; never empty)
 * @since 4.2.0
 */
@Evolving
public record TableDependency(String[] nameParts) implements Dependency {
  public TableDependency {
    Objects.requireNonNull(nameParts, "nameParts must not be null");
    if (nameParts.length == 0) {
      throw new IllegalArgumentException("nameParts must not be empty");
    }
    nameParts = nameParts.clone();
  }

  /** Returns a defensive copy of the underlying parts array. */
  @Override
  public String[] nameParts() {
    return nameParts.clone();
  }
}

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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.constraints.Constraint;
import org.apache.spark.sql.connector.expressions.Transform;

/**
 * A concrete {@link Table} that adapts a {@link TableInfo} -- it contains only table metadata and
 * defers read/write to Spark, which resolves the table provider into a data source at read time.
 * <p>
 * Catalogs build the metadata via {@link TableInfo.Builder} and return a {@code DelegatingTable}
 * from {@link TableCatalog#loadTable(Identifier)} (or {@link RelationCatalog#loadRelation} for a
 * data-source table) when they want Spark to handle the underlying source. A catalog that has its
 * own {@link Table} object returns that instead. Views are never represented as a
 * {@code DelegatingTable}: a view is a {@link View}, which is itself a {@link Relation}.
 *
 * @since 4.2.0
 */
@Evolving
public class DelegatingTable implements Table {
  private final TableInfo info;
  private final String name;

  /**
   * @param info the table metadata to delegate to.
   * @param name human-readable name for this table, returned by {@link #name()} and surfaced
   *             in places that read it (e.g. {@code BatchScan} plan-tree labels and
   *             partition-management error messages). {@code DESCRIBE TABLE EXTENDED} does
   *             not read this field; it emits the resolved identifier as structured
   *             {@code Catalog} / {@code Namespace} / {@code Table} rows. Catalogs returning
   *             a {@code DelegatingTable} from {@link TableCatalog#loadTable} or
   *             {@link RelationCatalog#loadRelation} should typically pass
   *             {@code ident.toString()}, matching the quoted multi-part form used elsewhere
   *             for v2 identifiers.
   */
  public DelegatingTable(TableInfo info, String name) {
    this.info = Objects.requireNonNull(info, "info should not be null");
    this.name = Objects.requireNonNull(name, "name should not be null");
  }

  public TableInfo getTableInfo() {
    return info;
  }

  @Override
  public Column[] columns() {
    return info.columns();
  }

  @Override
  public Map<String, String> properties() {
    return Collections.unmodifiableMap(info.properties());
  }

  @Override
  public Transform[] partitioning() {
    return info.partitions();
  }

  @Override
  public Constraint[] constraints() {
    return info.constraints();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of();
  }
}

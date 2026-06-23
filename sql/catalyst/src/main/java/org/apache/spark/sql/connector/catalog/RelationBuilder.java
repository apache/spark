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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.StructType;

/**
 * Shared builder state for {@link TableInfo.Builder} and {@link View.Builder} -- the fields and
 * convenience setters common to building a table's metadata and a view: the columns and the
 * string properties bag, plus the reserved-property setters that apply to both. Setters return
 * {@code B} so subclass builders chain through their own type without a covariant override on each
 * inherited setter.
 * <p>
 * This is an internal implementation detail: tables and views do not share a value type (a
 * {@link TableInfo} describes a table Spark will realize as a {@link Table}, while a {@link View}
 * is itself a {@link Relation}), only this builder logic.
 */
abstract class RelationBuilder<B extends RelationBuilder<B>> {
  protected Column[] columns = new Column[0];
  protected Map<String, String> properties = new HashMap<>();

  protected abstract B self();

  public B withColumns(Column[] columns) {
    this.columns = columns;
    return self();
  }

  public B withSchema(StructType schema) {
    this.columns = CatalogV2Util.structTypeToV2Columns(schema);
    return self();
  }

  /**
   * Replaces the current properties map with a defensive copy of the given map. Any reserved
   * keys set earlier via convenience setters (e.g. {@link #withComment}) are discarded --
   * call those setters <i>after</i> this method, not before.
   */
  public B withProperties(Map<String, String> properties) {
    this.properties = new HashMap<>(properties);
    return self();
  }

  // Convenience setters below write reserved keys into the current `properties` map. Pair
  // each with a preceding `withProperties(...)` call if you want to start from a user map;
  // calling `withProperties` after a convenience setter discards the value the convenience
  // setter wrote.

  public B withComment(String comment) {
    properties.put(TableCatalog.PROP_COMMENT, comment);
    return self();
  }

  public B withCollation(String collation) {
    properties.put(TableCatalog.PROP_COLLATION, collation);
    return self();
  }

  public B withOwner(String owner) {
    properties.put(TableCatalog.PROP_OWNER, owner);
    return self();
  }

  public B withTableType(String tableType) {
    properties.put(TableCatalog.PROP_TABLE_TYPE, tableType);
    return self();
  }
}

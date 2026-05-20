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

import java.util.Map;
import javax.annotation.Nullable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.connector.ColumnImpl;
import org.apache.spark.sql.types.DataType;

/**
 * An interface representing a column of a {@link Table}. It defines basic properties of a column,
 * such as name and data type, as well as some advanced ones like default column value.
 * <p>
 * Data Sources do not need to implement it. They should consume it in APIs like
 * {@link TableCatalog#createTable(Identifier, Column[], Transform[], Map)}, and report it in
 * {@link Table#columns()} by calling the static {@code create} functions of this interface to
 * create it.
 * <p>
 * A column cannot have both a default value and a generation expression.
 */
@Evolving
public interface Column {

  static Column create(String name, DataType dataType) {
    return create(name, dataType, true);
  }

  static Column create(String name, DataType dataType, boolean nullable) {
    return create(name, dataType, nullable, null, null);
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      String metadataInJSON) {
    return new ColumnImpl(
        name,
        dataType,
        nullable,
        comment,
        /* defaultValue = */ null,
        /* generationExpression = */ null,
        /* identityColumnSpec = */ null,
        metadataInJSON,
        /* id = */ null);
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      ColumnDefaultValue defaultValue,
      String metadataInJSON) {
    return new ColumnImpl(
        name,
        dataType,
        nullable,
        comment,
        defaultValue,
        /* generationExpression = */ null,
        /* identityColumnSpec = */ null,
        metadataInJSON,
        /* id = */ null);
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      String generationExpression,
      String metadataInJSON) {
    return new ColumnImpl(
        name,
        dataType,
        nullable,
        comment,
        /* defaultValue = */ null,
        generationExpression,
        /* identityColumnSpec = */ null,
        metadataInJSON,
        /* id = */ null);
  }

  static Column create(
          String name,
          DataType dataType,
          boolean nullable,
          String comment,
          IdentityColumnSpec identityColumnSpec,
          String metadataInJSON) {
    return new ColumnImpl(
        name,
        dataType,
        nullable,
        comment,
        /* defaultValue = */ null,
        /* generationExpression = */ null,
        identityColumnSpec,
        metadataInJSON,
        /* id = */ null);
  }

  /**
   * Returns the name of this table column.
   */
  String name();

  /**
   * Returns the data type of this table column.
   */
  DataType dataType();

  /**
   * Returns true if this column may produce null values.
   */
  boolean nullable();

  /**
   * Returns the comment of this table column. Null means no comment.
   */
  @Nullable
  String comment();

  /**
   * Returns the default value of this table column. Null means no default value.
   */
  @Nullable
  ColumnDefaultValue defaultValue();

  /**
   * Returns the generation expression of this table column. Null means no generation expression.
   * <p>
   * The generation expression is stored as spark SQL dialect. It is up to the data source to verify
   * expression compatibility and reject writes as necessary.
   */
  @Nullable
  String generationExpression();

  /**
   * Returns the identity column specification of this table column. Null means no identity column.
   */
  @Nullable
  IdentityColumnSpec identityColumnSpec();

  /**
   * Returns the column metadata in JSON format.
   */
  @Nullable
  String metadataInJSON();

  /**
   * Returns the ID of this top-level column, or null. The ID is an opt-in identifier that the
   * connector uses to track column identity beyond column name and type.
   * <p>
   * When a non-null ID is returned, the connector commits to the following contract:
   * <ul>
   *   <li>The ID is stable across renames (logical name changes preserve the ID).</li>
   *   <li>The ID changes when a top-level column is dropped and re-added, even with the same
   *       name and type.</li>
   *   <li>IDs are not reused within a table's history.</li>
   * </ul>
   * <p>
   * When null is returned, Spark skips identity validation for that column. Connectors should
   * return null when:
   * <ul>
   *   <li>The catalog has no notion of column identity beyond name and type, OR</li>
   *   <li>The connector chooses to treat same-name drop+re-add as the same column
   *       (lenient semantics).</li>
   * </ul>
   * Returning null is per-column: a connector may return IDs for some columns and null for
   * others.
   * <p>
   * This API covers top-level columns only. Nested struct fields, array elements, and map
   * keys/values do not have separate IDs. Connectors that track nested field IDs can encode
   * them into the returned top-level Column ID string to detect nested changes, since Spark
   * only compares string equality.
   */
  @Nullable
  default String id() {
    return null;
  }
}

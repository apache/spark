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
    return new ColumnImpl(name, dataType, nullable, comment, null, null, null, metadataInJSON);
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      ColumnDefaultValue defaultValue,
      String metadataInJSON) {
    return new ColumnImpl(name, dataType, nullable, comment, defaultValue,
            null, null, metadataInJSON);
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      String generationExpression,
      String metadataInJSON) {
    return new ColumnImpl(name, dataType, nullable, comment, null,
            generationExpression, null, metadataInJSON);
  }

  static Column create(
          String name,
          DataType dataType,
          boolean nullable,
          String comment,
          IdentityColumnSpec identityColumnSpec,
          String metadataInJSON) {
    return new ColumnImpl(name, dataType, nullable, comment, null,
            null, identityColumnSpec, metadataInJSON);
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
}

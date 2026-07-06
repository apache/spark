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
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.connector.ColumnImpl;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.util.SchemaUtils;

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
    return builderFor(name, dataType).build();
  }

  static Column create(String name, DataType dataType, boolean nullable) {
    return builderFor(name, dataType).nullable(nullable).build();
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      String metadataInJSON) {
    return builderFor(name, dataType)
        .nullable(nullable)
        .comment(comment)
        .metadata(metadataInJSON)
        .build();
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      ColumnDefaultValue defaultValue,
      String metadataInJSON) {
    return builderFor(name, dataType)
        .nullable(nullable)
        .comment(comment)
        .defaultValue(defaultValue)
        .metadata(metadataInJSON)
        .build();
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      String generationExpression,
      String metadataInJSON) {
    return builderFor(name, dataType)
        .nullable(nullable)
        .comment(comment)
        .generationExpression(generationExpression)
        .metadata(metadataInJSON)
        .build();
  }

  static Column create(
      String name,
      DataType dataType,
      boolean nullable,
      String comment,
      IdentityColumnSpec identityColumnSpec,
      String metadataInJSON) {
    return builderFor(name, dataType)
        .nullable(nullable)
        .comment(comment)
        .identityColumnSpec(identityColumnSpec)
        .metadata(metadataInJSON)
        .build();
  }

  /**
   * Creates a builder for a new column with the given name and data type.
   *
   * @param name the name of the column
   * @param dataType the data type of the column
   * @return a new builder
   * @since 4.2.0
   */
  static Builder builderFor(String name, DataType dataType) {
    return new Builder(name, dataType);
  }

  /**
   * Creates a builder with pre-populated info from an existing column.
   *
   * @param column the source column
   * @return a new builder seeded with the column's current state
   * @since 4.2.0
   */
  static Builder builderFrom(Column column) {
    return new Builder(column.name(), column.dataType())
        .nullable(column.nullable())
        .comment(column.comment())
        .defaultValue(column.defaultValue())
        .generationExpression(column.columnGenerationExpression())
        .identityColumnSpec(column.identityColumnSpec())
        .metadata(column.metadataInJSON())
        .id(column.id());
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
   * Returns the generation expression of this table column as a SQL string. Null means no
   * generation expression.
   * <p>
   * This returns only the SQL string form. Prefer {@link #columnGenerationExpression()}, which can
   * also carry a connector {@link org.apache.spark.sql.connector.expressions.Expression} and
   * captures the semantics unambiguously. It is up to the data source to verify expression
   * compatibility and reject writes as necessary.
   */
  @Nullable
  default String generationExpression() {
    return columnGenerationExpression() != null ? columnGenerationExpression().getSql() : null;
  }

  /**
   * Returns the generation expression of this table column as a {@link GenerationExpression}.
   * Null means no generation expression.
   *
   * @since 4.3.0
   */
  @Nullable
  default GenerationExpression columnGenerationExpression() {
    return null;
  }

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
   * keys/values carry their own IDs in struct field metadata. Spark validates both top-level and
   * nested struct field IDs as part of schema compatibility checks (array elements and map/key
   * values' validation is not supported yet). See {@link StructField#id()}.
   */
  @Nullable
  default String id() {
    return null;
  }

  /**
   * A builder for {@link Column}.
   *
   * @since 4.2.0
   */
  class Builder {
    private final String name;
    private DataType dataType;
    private boolean nullable = true;
    private String comment = null;
    private ColumnDefaultValue defaultValue = null;
    private GenerationExpression genExpr = null;
    private IdentityColumnSpec identityColumnSpec = null;
    private String metadataInJSON = null;
    private String id = null;

    private Builder(String name, DataType dataType) {
      this.name = Objects.requireNonNull(name, "name must not be null");
      this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
    }

    public Builder nullable(boolean nullable) {
      this.nullable = nullable;
      return this;
    }

    public Builder comment(String comment) {
      this.comment = comment;
      return this;
    }

    public Builder defaultValue(ColumnDefaultValue defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder generationExpression(String sql) {
      this.genExpr = sql != null ? new GenerationExpression(sql) : null;
      return this;
    }

    public Builder generationExpression(GenerationExpression generationExpr) {
      this.genExpr = generationExpr;
      return this;
    }

    public Builder identityColumnSpec(IdentityColumnSpec identityColumnSpec) {
      this.identityColumnSpec = identityColumnSpec;
      return this;
    }

    public Builder metadata(String metadataInJSON) {
      this.metadataInJSON = metadataInJSON;
      return this;
    }

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder clearIds() {
      this.id = null;
      this.dataType = SchemaUtils.clearFieldIds(dataType);
      return this;
    }

    public Column build() {
      validateState();
      return new ColumnImpl(
          name, dataType, nullable, comment, defaultValue,
          genExpr, identityColumnSpec, metadataInJSON, id);
    }

    private void validateState() {
      if (hasConflictingDefinitions()) {
        throw new SparkIllegalArgumentException(
            "INTERNAL_ERROR",
            Map.of("message",
                "Column '" + name + "' cannot have more than one definition of: " +
                "default value, generation expression, identity column spec"));
      }
    }

    private boolean hasConflictingDefinitions() {
      long definitionCount = Stream.of(defaultValue, genExpr, identityColumnSpec)
          .filter(Objects::nonNull)
          .count();
      return definitionCount > 1;
    }
  }
}

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

import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.types.DataType;

/**
 * TableChange subclasses represent requested changes to a table. These are passed to
 * {@link TableCatalog#alterTable}. For example,
 * <pre>
 *   import TableChange._
 *   val catalog = Catalogs.load(name)
 *   catalog.asTableCatalog.alterTable(ident,
 *       addColumn("x", IntegerType),
 *       renameColumn("a", "b"),
 *       deleteColumn("c")
 *     )
 * </pre>
 *
 * @since 3.0.0
 */
@Evolving
public interface TableChange {

  /**
   * Create a TableChange for setting a table property.
   * <p>
   * If the property already exists, it will be replaced with the new value.
   *
   * @param property the property name
   * @param value the new property value
   * @return a TableChange for the addition
   */
  static TableChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Create a TableChange for removing a table property.
   * <p>
   * If the property does not exist, the change will succeed.
   *
   * @param property the property name
   * @return a TableChange for the addition
   */
  static TableChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Create a TableChange for adding an optional column.
   * <p>
   * If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @return a TableChange for the addition
   */
  static TableChange addColumn(String[] fieldNames, DataType dataType) {
    return new AddColumn(fieldNames, dataType, true, null, null, null);
  }

  /**
   * Create a TableChange for adding a column.
   * <p>
   * If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @param isNullable whether the new column can contain null
   * @return a TableChange for the addition
   */
  static TableChange addColumn(String[] fieldNames, DataType dataType, boolean isNullable) {
    return new AddColumn(fieldNames, dataType, isNullable, null, null, null);
  }

  /**
   * Create a TableChange for adding a column.
   * <p>
   * If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @param isNullable whether the new column can contain null
   * @param comment the new field's comment string
   * @return a TableChange for the addition
   */
  static TableChange addColumn(
      String[] fieldNames,
      DataType dataType,
      boolean isNullable,
      String comment) {
    return new AddColumn(fieldNames, dataType, isNullable, comment, null, null);
  }

  /**
   * Create a TableChange for adding a column.
   * <p>
   * If the field already exists, the change will result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change will
   * result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @param isNullable whether the new column can contain null
   * @param comment the new field's comment string
   * @param position the new columns's position
   * @param defaultValue default value to return when scanning from the new column, if any
   * @return a TableChange for the addition
   */
  static TableChange addColumn(
      String[] fieldNames,
      DataType dataType,
      boolean isNullable,
      String comment,
      ColumnPosition position,
      ColumnDefaultValue defaultValue) {
    return new AddColumn(fieldNames, dataType, isNullable, comment, position, defaultValue);
  }

  /**
   * Create a TableChange for renaming a field.
   * <p>
   * The name is used to find the field to rename. The new name will replace the leaf field name.
   * For example, renameColumn(["a", "b", "c"], "x") should produce column a.b.x.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames the current field names
   * @param newName the new name
   * @return a TableChange for the rename
   */
  static TableChange renameColumn(String[] fieldNames, String newName) {
    return new RenameColumn(fieldNames, newName);
  }

  /**
   * Create a TableChange for updating the type of a field that is nullable.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to update
   * @param newDataType the new data type
   * @return a TableChange for the update
   */
  static TableChange updateColumnType(String[] fieldNames, DataType newDataType) {
    return new UpdateColumnType(fieldNames, newDataType);
  }

  /**
   * Create a TableChange for updating the nullability of a field.
   * <p>
   * The name is used to find the field to update.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to update
   * @param nullable the nullability
   * @return a TableChange for the update
   */
  static TableChange updateColumnNullability(String[] fieldNames, boolean nullable) {
    return new UpdateColumnNullability(fieldNames, nullable);
  }

  /**
   * Create a TableChange for updating the comment of a field.
   * <p>
   * The name is used to find the field to update.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to update
   * @param newComment the new comment
   * @return a TableChange for the update
   */
  static TableChange updateColumnComment(String[] fieldNames, String newComment) {
    return new UpdateColumnComment(fieldNames, newComment);
  }

  /**
   * Create a TableChange for updating the position of a field.
   * <p>
   * The name is used to find the field to update.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to update
   * @param newPosition the new position
   * @return a TableChange for the update
   */
  static TableChange updateColumnPosition(String[] fieldNames, ColumnPosition newPosition) {
    return new UpdateColumnPosition(fieldNames, newPosition);
  }

  /**
   * Create a TableChange for updating the default value of a field.
   * <p>
   * The name is used to find the field to update.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to update
   * @param newDefaultValue the new default value SQL string (Spark SQL dialect).
   * @return a TableChange for the update
   */
  static TableChange updateColumnDefaultValue(String[] fieldNames, String newDefaultValue) {
    return new UpdateColumnDefaultValue(fieldNames, newDefaultValue);
  }

  /**
   * Create a TableChange for deleting a field.
   * <p>
   * If the field does not exist, the change will result in an {@link IllegalArgumentException}.
   *
   * @param fieldNames field names of the column to delete
   * @param ifExists   silence the error if column doesn't exist during drop
   * @return a TableChange for the delete
   */
  static TableChange deleteColumn(String[] fieldNames, Boolean ifExists) {
    return new DeleteColumn(fieldNames, ifExists);
  }

  /**
   * Create a TableChange for changing clustering columns for a table.
   *
   * @param clusteringColumns clustering columns to change to. Each clustering column represents
   *                          field names.
   * @return a TableChange for this assignment
   */
  static TableChange clusterBy(NamedReference[] clusteringColumns) {
    return new ClusterBy(clusteringColumns);
  }

  /**
   * A TableChange to set a table property.
   * <p>
   * If the property already exists, it must be replaced with the new value.
   */
  final class SetProperty implements TableChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    public String property() {
      return property;
    }

    public String value() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return property.equals(that.property) &&
        value.equals(that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }
  }

  /**
   * A TableChange to remove a table property.
   * <p>
   * If the property does not exist, the change should succeed.
   */
  final class RemoveProperty implements TableChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    public String property() {
      return property;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return property.equals(that.property);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property);
    }
  }

  interface ColumnPosition {

    static ColumnPosition first() {
      return First.INSTANCE;
    }

    static ColumnPosition after(String column) {
      return new After(column);
    }
  }

  /**
   * Column position FIRST means the specified column should be the first column.
   * Note that, the specified column may be a nested field, and then FIRST means this field should
   * be the first one within the struct.
   */
  final class First implements ColumnPosition {
    private static final First INSTANCE = new First();

    private First() {}

    @Override
    public String toString() {
      return "FIRST";
    }
  }

  /**
   * Column position AFTER means the specified column should be put after the given `column`.
   * Note that, the specified column may be a nested field, and then the given `column` refers to
   * a field in the same struct.
   */
  final class After implements ColumnPosition {
    private final String column;

    private After(String column) {
      assert column != null;
      this.column = column;
    }

    public String column() {
      return column;
    }

    @Override
    public String toString() {
      return "AFTER " + column;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      After after = (After) o;
      return column.equals(after.column);
    }

    @Override
    public int hashCode() {
      return Objects.hash(column);
    }
  }

  interface ColumnChange extends TableChange {
    String[] fieldNames();
  }

  /**
   * A TableChange to add a field. The implementation may need to back-fill all the existing data
   * to add this new column, or remember the column default value specified here and let the reader
   * fill the column value when reading existing data that do not have this new column.
   * <p>
   * If the field already exists, the change must result in an {@link IllegalArgumentException}.
   * If the new field is nested and its parent does not exist or is not a struct, the change must
   * result in an {@link IllegalArgumentException}.
   */
  final class AddColumn implements ColumnChange {
    private final String[] fieldNames;
    private final DataType dataType;
    private final boolean isNullable;
    private final String comment;
    private final ColumnPosition position;
    private final ColumnDefaultValue defaultValue;

    private AddColumn(
        String[] fieldNames,
        DataType dataType,
        boolean isNullable,
        String comment,
        ColumnPosition position,
        ColumnDefaultValue defaultValue) {
      this.fieldNames = fieldNames;
      this.dataType = dataType;
      this.isNullable = isNullable;
      this.comment = comment;
      this.position = position;
      this.defaultValue = defaultValue;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public DataType dataType() {
      return dataType;
    }

    public boolean isNullable() {
      return isNullable;
    }

    @Nullable
    public String comment() {
      return comment;
    }

    @Nullable
    public ColumnPosition position() {
      return position;
    }

    @Nullable
    public ColumnDefaultValue defaultValue() { return defaultValue; }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddColumn addColumn = (AddColumn) o;
      return isNullable == addColumn.isNullable &&
        Arrays.equals(fieldNames, addColumn.fieldNames) &&
        dataType.equals(addColumn.dataType) &&
        Objects.equals(comment, addColumn.comment) &&
        Objects.equals(position, addColumn.position) &&
        Objects.equals(defaultValue, addColumn.defaultValue);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(dataType, isNullable, comment, position);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to rename a field.
   * <p>
   * The name is used to find the field to rename. The new name will replace the leaf field name.
   * For example, renameColumn("a.b.c", "x") should produce column a.b.x.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class RenameColumn implements ColumnChange {
    private final String[] fieldNames;
    private final String newName;

    private RenameColumn(String[] fieldNames, String newName) {
      this.fieldNames = fieldNames;
      this.newName = newName;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public String newName() {
      return newName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameColumn that = (RenameColumn) o;
      return Arrays.equals(fieldNames, that.fieldNames) &&
        newName.equals(that.newName);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(newName);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to update the type of a field.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class UpdateColumnType implements ColumnChange {
    private final String[] fieldNames;
    private final DataType newDataType;

    private UpdateColumnType(String[] fieldNames, DataType newDataType) {
      this.fieldNames = fieldNames;
      this.newDataType = newDataType;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public DataType newDataType() {
      return newDataType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnType that = (UpdateColumnType) o;
      return Arrays.equals(fieldNames, that.fieldNames) &&
        newDataType.equals(that.newDataType);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(newDataType);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to update the nullability of a field.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class UpdateColumnNullability implements ColumnChange {
    private final String[] fieldNames;
    private final boolean nullable;

    private UpdateColumnNullability(String[] fieldNames, boolean nullable) {
      this.fieldNames = fieldNames;
      this.nullable = nullable;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public boolean nullable() {
      return nullable;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnNullability that = (UpdateColumnNullability) o;
      return nullable == that.nullable &&
        Arrays.equals(fieldNames, that.fieldNames);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(nullable);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to update the comment of a field.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class UpdateColumnComment implements ColumnChange {
    private final String[] fieldNames;
    private final String newComment;

    private UpdateColumnComment(String[] fieldNames, String newComment) {
      this.fieldNames = fieldNames;
      this.newComment = newComment;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public String newComment() {
      return newComment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnComment that = (UpdateColumnComment) o;
      return Arrays.equals(fieldNames, that.fieldNames) &&
        newComment.equals(that.newComment);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(newComment);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to update the position of a field.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class UpdateColumnPosition implements ColumnChange {
    private final String[] fieldNames;
    private final ColumnPosition position;

    private UpdateColumnPosition(String[] fieldNames, ColumnPosition position) {
      this.fieldNames = fieldNames;
      this.position = position;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public ColumnPosition position() {
      return position;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnPosition that = (UpdateColumnPosition) o;
      return Arrays.equals(fieldNames, that.fieldNames) &&
        position.equals(that.position);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(position);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to update the default value of a field.
   * <p>
   * The field names are used to find the field to update.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class UpdateColumnDefaultValue implements ColumnChange {
    private final String[] fieldNames;
    private final String newDefaultValue;

    private UpdateColumnDefaultValue(String[] fieldNames, String newDefaultValue) {
      this.fieldNames = fieldNames;
      this.newDefaultValue = newDefaultValue;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    /**
     * Returns the column default value SQL string (Spark SQL dialect). The default value literal
     * is not provided as updating column default values does not need to back-fill existing data.
     * Empty string means dropping the column default value.
     */
    public String newDefaultValue() { return newDefaultValue; }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateColumnDefaultValue that = (UpdateColumnDefaultValue) o;
      return Arrays.equals(fieldNames, that.fieldNames) &&
        newDefaultValue.equals(that.newDefaultValue());
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(newDefaultValue);
      result = 31 * result + Arrays.hashCode(fieldNames);
      return result;
    }
  }

  /**
   * A TableChange to delete a field.
   * <p>
   * If the field does not exist, the change must result in an {@link IllegalArgumentException}.
   */
  final class DeleteColumn implements ColumnChange {
    private final String[] fieldNames;
    private final Boolean ifExists;

    private DeleteColumn(String[] fieldNames, Boolean ifExists) {
      this.fieldNames = fieldNames;
      this.ifExists = ifExists;
    }

    @Override
    public String[] fieldNames() {
      return fieldNames;
    }

    public Boolean ifExists() { return ifExists; }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DeleteColumn that = (DeleteColumn) o;
      return Arrays.equals(fieldNames, that.fieldNames) && that.ifExists() == this.ifExists();
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(fieldNames);
    }
  }

  /** A TableChange to alter clustering columns for a table. */
  final class ClusterBy implements TableChange {
    private final NamedReference[] clusteringColumns;

    private ClusterBy(NamedReference[] clusteringColumns) {
      this.clusteringColumns = clusteringColumns;
    }

    public NamedReference[] clusteringColumns() { return clusteringColumns; }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ClusterBy that = (ClusterBy) o;
      return Arrays.equals(clusteringColumns, that.clusteringColumns());
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(clusteringColumns);
    }
  }
}

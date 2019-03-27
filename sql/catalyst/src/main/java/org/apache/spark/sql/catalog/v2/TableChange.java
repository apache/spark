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

package org.apache.spark.sql.catalog.v2;

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
 */
public interface TableChange {

  /**
   * Create a TableChange for setting a table property.
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
   *
   * @param property the property name
   * @return a TableChange for the addition
   */
  static TableChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Create a TableChange for adding an optional column to a table.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @return a TableChange for the addition
   */
  static TableChange addColumn(String[] fieldNames, DataType dataType) {
    return new AddColumn(fieldNames, dataType, true, null);
  }

  /**
   * Create a TableChange for adding a column to a table.
   *
   * @param fieldNames field names of the new column
   * @param dataType the new column's data type
   * @param isNullable whether the new column can contain null
   * @return a TableChange for the addition
   */
  static TableChange addColumn(String[] fieldNames, DataType dataType, boolean isNullable) {
    return new AddColumn(fieldNames, dataType, isNullable, null);
  }

  /**
   * Create a TableChange for adding a top-level column to a table.
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
    return new AddColumn(fieldNames, dataType, isNullable, comment);
  }

  /**
   * Create a TableChange for renaming a field.
   * <p>
   * The name is used to find the field to rename. The new name will replace the leaf field name.
   * For example, renameColumn("a.b.c", "x") should produce column a.b.x.
   *
   * @param fieldNames the current field names
   * @param newName the new name
   * @return a TableChange for the rename
   */
  static TableChange renameColumn(String[] fieldNames, String newName) {
    return new RenameColumn(fieldNames, newName);
  }

  /**
   * Create a TableChange for updating the type of a field.
   * <p>
   * The field names are used to find the field to update.
   *
   * @param fieldNames field names of the column to update
   * @param newDataType the new data type
   * @return a TableChange for the update
   */
  static TableChange updateColumn(String[] fieldNames, DataType newDataType) {
    return new UpdateColumn(fieldNames, newDataType, true);
  }

  /**
   * Create a TableChange for updating the type of a field.
   * <p>
   * The field names are used to find the field to update.
   *
   * @param fieldNames field names of the column to update
   * @param newDataType the new data type
   * @return a TableChange for the update
   */
  static TableChange updateColumn(String[] fieldNames, DataType newDataType, boolean isNullable) {
    return new UpdateColumn(fieldNames, newDataType, isNullable);
  }

  /**
   * Create a TableChange for updating the comment of a field.
   * <p>
   * The name is used to find the field to update.
   *
   * @param fieldNames field names of the column to update
   * @param newComment the new comment
   * @return a TableChange for the update
   */
  static TableChange updateComment(String[] fieldNames, String newComment) {
    return new UpdateColumnComment(fieldNames, newComment);
  }

  /**
   * Create a TableChange for deleting a field from a table.
   *
   * @param fieldNames field names of the column to delete
   * @return a TableChange for the delete
   */
  static TableChange deleteColumn(String[] fieldNames) {
    return new DeleteColumn(fieldNames);
  }

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
  }

  final class RemoveProperty implements TableChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    public String property() {
      return property;
    }
  }

  final class AddColumn implements TableChange {
    private final String[] fieldNames;
    private final DataType dataType;
    private final boolean isNullable;
    private final String comment;

    private AddColumn(String[] fieldNames, DataType dataType, boolean isNullable, String comment) {
      this.fieldNames = fieldNames;
      this.dataType = dataType;
      this.isNullable = isNullable;
      this.comment = comment;
    }

    public String[] fieldNames() {
      return fieldNames;
    }

    public DataType dataType() {
      return dataType;
    }

    public boolean isNullable() {
      return isNullable;
    }

    public String comment() {
      return comment;
    }
  }

  final class RenameColumn implements TableChange {
    private final String[] fieldNames;
    private final String newName;

    private RenameColumn(String[] fieldNames, String newName) {
      this.fieldNames = fieldNames;
      this.newName = newName;
    }

    public String[] fieldNames() {
      return fieldNames;
    }

    public String newName() {
      return newName;
    }
  }

  final class UpdateColumn implements TableChange {
    private final String[] fieldNames;
    private final DataType newDataType;
    private final boolean isNullable;

    private UpdateColumn(String[] fieldNames, DataType newDataType, boolean isNullable) {
      this.fieldNames = fieldNames;
      this.newDataType = newDataType;
      this.isNullable = isNullable;
    }

    public String[] fieldNames() {
      return fieldNames;
    }

    public DataType newDataType() {
      return newDataType;
    }

    public boolean isNullable() {
      return isNullable;
    }
  }

  final class UpdateColumnComment implements TableChange {
    private final String[] fieldNames;
    private final String newComment;

    private UpdateColumnComment(String[] fieldNames, String newComment) {
      this.fieldNames = fieldNames;
      this.newComment = newComment;
    }

    public String[] fieldNames() {
      return fieldNames;
    }

    public String newComment() {
      return newComment;
    }
  }

  final class DeleteColumn implements TableChange {
    private final String[] fieldNames;

    private DeleteColumn(String[] fieldNames) {
      this.fieldNames = fieldNames;
    }

    public String[] fieldNames() {
      return fieldNames;
    }
  }

}

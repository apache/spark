/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
 *   val catalog = source.asInstanceOf[TableSupport].catalog()
 *   catalog.alterTable(ident,
 *       addColumn("x", IntegerType),
 *       renameColumn("a", "b"),
 *       deleteColumn("c")
 *     )
 * </pre>
 */
public interface TableChange {

  /**
   * Create a TableChange for adding a top-level column to a table.
   * <p>
   * Because "." may be interpreted as a field path separator or may be used in field names, it is
   * not allowed in names passed to this method. To add to nested types or to add fields with
   * names that contain ".", use {@link #addColumn(String, String, DataType)}.
   *
   * @param name the new top-level column name
   * @param dataType the new column's data type
   * @return a TableChange for the addition
   */
  static TableChange addColumn(String name, DataType dataType) {
    return new AddColumn(null, name, dataType);
  }

  /**
   * Create a TableChange for adding a nested column to a table.
   * <p>
   * The parent name is used to find the parent struct type where the nested field will be added.
   * If the parent name is null, the new column will be added to the root as a top-level column.
   * If parent identifies a struct, a new column is added to that struct. If it identifies a list,
   * the column is added to the list element struct, and if it identifies a map, the new column is
   * added to the map's value struct.
   * <p>
   * The given name is used to name the new column and names containing "." are not handled
   * differently.
   *
   * @param parent the new field's parent
   * @param name the new field name
   * @param dataType the new field's data type
   * @return a TableChange for the addition
   */
  static TableChange addColumn(String parent, String name, DataType dataType) {
    return new AddColumn(parent, name, dataType);
  }

  /**
   * Create a TableChange for renaming a field.
   * <p>
   * The name is used to find the field to rename. The new name will replace the name of the type.
   * For example, renameColumn("a.b.c", "x") should produce column a.b.x.
   *
   * @param name the current field name
   * @param newName the new name
   * @return a TableChange for the rename
   */
  static TableChange renameColumn(String name, String newName) {
    return new RenameColumn(name, newName);
  }

  /**
   * Create a TableChange for updating the type of a field.
   * <p>
   * The name is used to find the field to update.
   *
   * @param name the field name
   * @param newDataType the new data type
   * @return a TableChange for the update
   */
  static TableChange updateColumn(String name, DataType newDataType) {
    return new UpdateColumn(name, newDataType);
  }

  /**
   * Create a TableChange for deleting a field from a table.
   *
   * @param name the name of the field to delete
   * @return a TableChange for the delete
   */
  static TableChange deleteColumn(String name) {
    return new DeleteColumn(name);
  }

  final class AddColumn implements TableChange {
    private final String parent;
    private final String name;
    private final DataType dataType;

    private AddColumn(String parent, String name, DataType dataType) {
      this.parent = parent;
      this.name = name;
      this.dataType = dataType;
    }

    public String parent() {
      return parent;
    }

    public String name() {
      return name;
    }

    public DataType type() {
      return dataType;
    }
  }

  final class RenameColumn implements TableChange {
    private final String name;
    private final String newName;

    private RenameColumn(String name, String newName) {
      this.name = name;
      this.newName = newName;
    }

    public String name() {
      return name;
    }

    public String newName() {
      return newName;
    }
  }

  final class UpdateColumn implements TableChange {
    private final String name;
    private final DataType newDataType;

    private UpdateColumn(String name, DataType newDataType) {
      this.name = name;
      this.newDataType = newDataType;
    }

    public String name() {
      return name;
    }

    public DataType newDataType() {
      return newDataType;
    }
  }

  final class DeleteColumn implements TableChange {
    private final String name;

    private DeleteColumn(String name) {
      this.name = name;
    }

    public String name() {
      return name;
    }
  }

}

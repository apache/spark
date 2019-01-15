/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.TTableSchema;

/**
 * TableSchema.
 *
 */
public class TableSchema {
  private final List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();

  public TableSchema() {
  }

  public TableSchema(int numColumns) {
    // TODO: remove this constructor
  }

  public TableSchema(TTableSchema tTableSchema) {
    for (TColumnDesc tColumnDesc : tTableSchema.getColumns()) {
      columns.add(new ColumnDescriptor(tColumnDesc));
    }
  }

  public TableSchema(List<FieldSchema> fieldSchemas) {
    int pos = 1;
    for (FieldSchema field : fieldSchemas) {
      columns.add(new ColumnDescriptor(field.getName(), field.getComment(),
          new TypeDescriptor(field.getType()), pos++));
    }
  }

  public TableSchema(Schema schema) {
    this(schema.getFieldSchemas());
  }

  public List<ColumnDescriptor> getColumnDescriptors() {
    return new ArrayList<ColumnDescriptor>(columns);
  }

  public ColumnDescriptor getColumnDescriptorAt(int pos) {
    return columns.get(pos);
  }

  public int getSize() {
    return columns.size();
  }

  public void clear() {
    columns.clear();
  }


  public TTableSchema toTTableSchema() {
    TTableSchema tTableSchema = new TTableSchema();
    for (ColumnDescriptor col : columns) {
      tTableSchema.addToColumns(col.toTColumnDesc());
    }
    return tTableSchema;
  }

  public TypeDescriptor[] toTypeDescriptors() {
    TypeDescriptor[] types = new TypeDescriptor[columns.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = columns.get(i).getTypeDescriptor();
    }
    return types;
  }

  public TableSchema addPrimitiveColumn(String columnName, Type columnType, String columnComment) {
    columns.add(ColumnDescriptor.newPrimitiveColumnDescriptor(columnName, columnComment, columnType, columns.size() + 1));
    return this;
  }

  public TableSchema addStringColumn(String columnName, String columnComment) {
    columns.add(ColumnDescriptor.newPrimitiveColumnDescriptor(columnName, columnComment, Type.STRING_TYPE, columns.size() + 1));
    return this;
  }
}

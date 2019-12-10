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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.service.cli.thrift.TColumnDesc;


/**
 * ColumnDescriptor.
 *
 */
public class ColumnDescriptor {
  private final String name;
  private final String comment;
  private final TypeDescriptor type;
  // ordinal position of this column in the schema
  private final int position;

  public ColumnDescriptor(String name, String comment, TypeDescriptor type, int position) {
    this.name = name;
    this.comment = comment;
    this.type = type;
    this.position = position;
  }

  public ColumnDescriptor(TColumnDesc tColumnDesc) {
    name = tColumnDesc.getColumnName();
    comment = tColumnDesc.getComment();
    type = new TypeDescriptor(tColumnDesc.getTypeDesc());
    position = tColumnDesc.getPosition();
  }

  public ColumnDescriptor(FieldSchema column, int position) {
    name = column.getName();
    comment = column.getComment();
    type = new TypeDescriptor(column.getType());
    this.position = position;
  }

  public static ColumnDescriptor newPrimitiveColumnDescriptor(String name, String comment, Type type, int position) {
    // Current usage looks like it's only for metadata columns, but if that changes then
    // this method may need to require a type qualifiers aruments.
    return new ColumnDescriptor(name, comment, new TypeDescriptor(type), position);
  }

  public String getName() {
    return name;
  }

  public String getComment() {
    return comment;
  }

  public TypeDescriptor getTypeDescriptor() {
    return type;
  }

  public int getOrdinalPosition() {
    return position;
  }

  public TColumnDesc toTColumnDesc() {
    TColumnDesc tColumnDesc = new TColumnDesc();
    tColumnDesc.setColumnName(name);
    tColumnDesc.setComment(comment);
    tColumnDesc.setTypeDesc(type.toTTypeDesc());
    tColumnDesc.setPosition(position);
    return tColumnDesc;
  }

  public Type getType() {
    return type.getType();
  }

  public boolean isPrimitive() {
    return type.getType().isPrimitiveType();
  }

  public String getTypeName() {
    return type.getTypeName();
  }
}

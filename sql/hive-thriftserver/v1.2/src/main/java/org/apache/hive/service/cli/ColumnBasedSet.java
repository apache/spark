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
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;

/**
 * ColumnBasedSet.
 */
public class ColumnBasedSet implements RowSet {

  private long startOffset;

  private final Type[] types; // non-null only for writing (server-side)
  private final List<Column> columns;

  public ColumnBasedSet(TableSchema schema) {
    types = schema.toTypes();
    columns = new ArrayList<Column>();
    for (ColumnDescriptor colDesc : schema.getColumnDescriptors()) {
      columns.add(new Column(colDesc.getType()));
    }
  }

  public ColumnBasedSet(TRowSet tRowSet) {
    types = null;
    columns = new ArrayList<Column>();
    for (TColumn tvalue : tRowSet.getColumns()) {
      columns.add(new Column(tvalue));
    }
    startOffset = tRowSet.getStartRowOffset();
  }

  private ColumnBasedSet(Type[] types, List<Column> columns, long startOffset) {
    this.types = types;
    this.columns = columns;
    this.startOffset = startOffset;
  }

  @Override
  public ColumnBasedSet addRow(Object[] fields) {
    for (int i = 0; i < fields.length; i++) {
      columns.get(i).addValue(types[i], fields[i]);
    }
    return this;
  }

  public List<Column> getColumns() {
    return columns;
  }

  @Override
  public int numColumns() {
    return columns.size();
  }

  @Override
  public int numRows() {
    return columns.isEmpty() ? 0 : columns.get(0).size();
  }

  @Override
  public ColumnBasedSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);

    List<Column> subset = new ArrayList<Column>();
    for (int i = 0; i < columns.size(); i++) {
      subset.add(columns.get(i).extractSubset(0, numRows));
    }
    ColumnBasedSet result = new ColumnBasedSet(types, subset, startOffset);
    startOffset += numRows;
    return result;
  }

  @Override
  public long getStartOffset() {
    return startOffset;
  }

  @Override
  public void setStartOffset(long startOffset) {
    this.startOffset = startOffset;
  }

  public TRowSet toTRowSet() {
    TRowSet tRowSet = new TRowSet(startOffset, new ArrayList<TRow>());
    for (int i = 0; i < columns.size(); i++) {
      tRowSet.addToColumns(columns.get(i).toTColumn());
    }
    return tRowSet;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      private int index;
      private final Object[] convey = new Object[numColumns()];

      @Override
      public boolean hasNext() {
        return index < numRows();
      }

      @Override
      public Object[] next() {
        for (int i = 0; i < columns.size(); i++) {
          convey[i] = columns.get(i).get(index);
        }
        index++;
        return convey;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public Object[] fill(int index, Object[] convey) {
    for (int i = 0; i < columns.size(); i++) {
      convey[i] = columns.get(i).get(index);
    }
    return convey;
  }
}

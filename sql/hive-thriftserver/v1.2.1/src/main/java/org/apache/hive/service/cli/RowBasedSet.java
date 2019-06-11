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

import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;

/**
 * RowBasedSet
 */
public class RowBasedSet implements RowSet {

  private long startOffset;

  private final Type[] types; // non-null only for writing (server-side)
  private final RemovableList<TRow> rows;

  public RowBasedSet(TableSchema schema) {
    types = schema.toTypes();
    rows = new RemovableList<TRow>();
  }

  public RowBasedSet(TRowSet tRowSet) {
    types = null;
    rows = new RemovableList<TRow>(tRowSet.getRows());
    startOffset = tRowSet.getStartRowOffset();
  }

  private RowBasedSet(Type[] types, List<TRow> rows, long startOffset) {
    this.types = types;
    this.rows = new RemovableList<TRow>(rows);
    this.startOffset = startOffset;
  }

  @Override
  public RowBasedSet addRow(Object[] fields) {
    TRow tRow = new TRow();
    for (int i = 0; i < fields.length; i++) {
      tRow.addToColVals(ColumnValue.toTColumnValue(types[i], fields[i]));
    }
    rows.add(tRow);
    return this;
  }

  @Override
  public int numColumns() {
    return rows.isEmpty() ? 0 : rows.get(0).getColVals().size();
  }

  @Override
  public int numRows() {
    return rows.size();
  }

  public RowBasedSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);
    RowBasedSet result = new RowBasedSet(types, rows.subList(0, numRows), startOffset);
    rows.removeRange(0, numRows);
    startOffset += numRows;
    return result;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(long startOffset) {
    this.startOffset = startOffset;
  }

  public int getSize() {
    return rows.size();
  }

  public TRowSet toTRowSet() {
    TRowSet tRowSet = new TRowSet();
    tRowSet.setStartRowOffset(startOffset);
    tRowSet.setRows(new ArrayList<TRow>(rows));
    return tRowSet;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      final Iterator<TRow> iterator = rows.iterator();
      final Object[] convey = new Object[numColumns()];

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Object[] next() {
        TRow row = iterator.next();
        List<TColumnValue> values = row.getColVals();
        for (int i = 0; i < values.size(); i++) {
          convey[i] = ColumnValue.toColumnValue(values.get(i));
        }
        return convey;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  private static class RemovableList<E> extends ArrayList<E> {
    RemovableList() { super(); }
    RemovableList(List<E> rows) { super(rows); }
    @Override
    public void removeRange(int fromIndex, int toIndex) {
      super.removeRange(fromIndex, toIndex);
    }
  }
}

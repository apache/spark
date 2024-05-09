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

package org.apache.hive.service.cli;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.serde2.thrift.ColumnBuffer;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TRow;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import org.apache.spark.internal.Logger;
import org.apache.spark.internal.LoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;

/**
 * ColumnBasedSet.
 */
public class ColumnBasedSet implements RowSet {

  private long startOffset;

  private final TypeDescriptor[] descriptors; // non-null only for writing (server-side)
  private final List<ColumnBuffer> columns;
  private byte[] blob;
  private boolean isBlobBased = false;
  public static final Logger LOG = LoggerFactory.getLogger(ColumnBasedSet.class);

  public ColumnBasedSet(TableSchema schema) {
    descriptors = schema.toTypeDescriptors();
    columns = new ArrayList<ColumnBuffer>();
    for (ColumnDescriptor colDesc : schema.getColumnDescriptors()) {
      columns.add(new ColumnBuffer(colDesc.getType()));
    }
  }

  public ColumnBasedSet(TRowSet tRowSet) throws TException {
    descriptors = null;
    columns = new ArrayList<ColumnBuffer>();
    // Use TCompactProtocol to read serialized TColumns
    if (tRowSet.isSetBinaryColumns()) {
      TProtocol protocol =
          new TCompactProtocol(new TIOStreamTransport(new ByteArrayInputStream(
              tRowSet.getBinaryColumns())));
      // Read from the stream using the protocol for each column in final schema
      for (int i = 0; i < tRowSet.getColumnCount(); i++) {
        TColumn tvalue = new TColumn();
        try {
          tvalue.read(protocol);
        } catch (TException e) {
          LOG.error("{}", e, MDC.of(LogKeys.ERROR$.MODULE$, e.getMessage()));
          throw new TException("Error reading column value from the row set blob", e);
        }
        columns.add(new ColumnBuffer(tvalue));
      }
    }
    else {
      if (tRowSet.getColumns() != null) {
        for (TColumn tvalue : tRowSet.getColumns()) {
          columns.add(new ColumnBuffer(tvalue));
        }
      }
    }
    startOffset = tRowSet.getStartRowOffset();
  }

  private ColumnBasedSet(TypeDescriptor[] descriptors, List<ColumnBuffer> columns, long startOffset) {
    this.descriptors = descriptors;
    this.columns = columns;
    this.startOffset = startOffset;
  }

  public ColumnBasedSet(TableSchema schema, boolean isBlobBased) {
    this(schema);
    this.isBlobBased = isBlobBased;
  }

  @Override
  public ColumnBasedSet addRow(Object[] fields) {
    if (isBlobBased) {
      this.blob = (byte[]) fields[0];
    } else {
      for (int i = 0; i < fields.length; i++) {
        TypeDescriptor descriptor = descriptors[i];
        columns.get(i).addValue(descriptor.getType(), fields[i]);
      }
    }
    return this;
  }

  public List<ColumnBuffer> getColumns() {
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

    List<ColumnBuffer> subset = new ArrayList<ColumnBuffer>();
    for (int i = 0; i < columns.size(); i++) {
      subset.add(columns.get(i).extractSubset(numRows));
    }
    ColumnBasedSet result = new ColumnBasedSet(descriptors, subset, startOffset);
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
    if (isBlobBased) {
      tRowSet.setColumns(null);
      tRowSet.setBinaryColumns(blob);
      tRowSet.setColumnCount(numColumns());
    } else {
      for (int i = 0; i < columns.size(); i++) {
        tRowSet.addToColumns(columns.get(i).toTColumn());
      }
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
    };
  }

  public Object[] fill(int index, Object[] convey) {
    for (int i = 0; i < columns.size(); i++) {
      convey[i] = columns.get(i).get(index);
    }
    return convey;
  }
}

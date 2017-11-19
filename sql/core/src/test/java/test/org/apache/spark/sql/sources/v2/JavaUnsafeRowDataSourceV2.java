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

package test.org.apache.spark.sql.sources.v2;

import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2Options;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;

public class JavaUnsafeRowDataSourceV2 implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceV2Reader, SupportsScanUnsafeRow {
    private final StructType schema = new StructType().add("i", "int").add("j", "int");

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public List<ReadTask<UnsafeRow>> createUnsafeRowReadTasks() {
      return java.util.Arrays.asList(
        new JavaUnsafeRowReadTask(0, 5),
        new JavaUnsafeRowReadTask(5, 10));
    }
  }

  static class JavaUnsafeRowReadTask implements ReadTask<UnsafeRow>, DataReader<UnsafeRow> {
    private int start;
    private int end;
    private UnsafeRow row;

    JavaUnsafeRowReadTask(int start, int end) {
      this.start = start;
      this.end = end;
      this.row = new UnsafeRow(2);
      row.pointTo(new byte[8 * 3], 8 * 3);
    }

    @Override
    public DataReader<UnsafeRow> createDataReader() {
      return new JavaUnsafeRowReadTask(start - 1, end);
    }

    @Override
    public boolean next() {
      start += 1;
      return start < end;
    }

    @Override
    public UnsafeRow get() {
      row.setInt(0, start);
      row.setInt(1, -start);
      return row;
    }

    @Override
    public void close() throws IOException {

    }
  }

  @Override
  public DataSourceV2Reader createReader(DataSourceV2Options options) {
    return new Reader();
  }
}

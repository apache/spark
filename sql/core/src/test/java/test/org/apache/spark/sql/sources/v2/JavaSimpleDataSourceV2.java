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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2Options;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.ReadTask;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.types.StructType;

public class JavaSimpleDataSourceV2 implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceV2Reader {
    private final StructType schema = new StructType().add("i", "int").add("j", "int");

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public List<ReadTask<Row>> createReadTasks() {
      return java.util.Arrays.asList(
        new JavaSimpleReadTask(0, 5),
        new JavaSimpleReadTask(5, 10));
    }
  }

  static class JavaSimpleReadTask implements ReadTask<Row>, DataReader<Row> {
    private int start;
    private int end;

    JavaSimpleReadTask(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public DataReader<Row> createDataReader() {
      return new JavaSimpleReadTask(start - 1, end);
    }

    @Override
    public boolean next() {
      start += 1;
      return start < end;
    }

    @Override
    public Row get() {
      return new GenericRow(new Object[] {start, -start});
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

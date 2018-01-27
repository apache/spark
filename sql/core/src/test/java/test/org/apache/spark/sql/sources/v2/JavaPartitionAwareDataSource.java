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
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2Options;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;

public class JavaPartitionAwareDataSource implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceV2Reader, SupportsReportPartitioning {
    private final StructType schema = new StructType().add("a", "int").add("b", "int");

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public List<ReadTask<Row>> createReadTasks() {
      return java.util.Arrays.asList(
        new SpecificReadTask(new int[]{1, 1, 3}, new int[]{4, 4, 6}),
        new SpecificReadTask(new int[]{2, 4, 4}, new int[]{6, 2, 2}));
    }

    @Override
    public Partitioning outputPartitioning() {
      return new MyPartitioning();
    }
  }

  static class MyPartitioning implements Partitioning {

    @Override
    public int numPartitions() {
      return 2;
    }

    @Override
    public boolean satisfy(Distribution distribution) {
      if (distribution instanceof ClusteredDistribution) {
        String[] clusteredCols = ((ClusteredDistribution) distribution).clusteredColumns;
        return Arrays.asList(clusteredCols).contains("a");
      }

      return false;
    }
  }

  static class SpecificReadTask implements ReadTask<Row>, DataReader<Row> {
    private int[] i;
    private int[] j;
    private int current = -1;

    SpecificReadTask(int[] i, int[] j) {
      assert i.length == j.length;
      this.i = i;
      this.j = j;
    }

    @Override
    public boolean next() throws IOException {
      current += 1;
      return current < i.length;
    }

    @Override
    public Row get() {
      return new GenericRow(new Object[] {i[current], j[current]});
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public DataReader<Row> createDataReader() {
      return this;
    }
  }

  @Override
  public DataSourceV2Reader createReader(DataSourceV2Options options) {
    return new Reader();
  }
}

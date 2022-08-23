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

package test.org.apache.spark.sql.connector;

import java.io.IOException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.TestingV2Source;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class JavaPartitionAwareDataSource implements TestingV2Source {

  static class MyScanBuilder extends JavaSimpleScanBuilder implements SupportsReportPartitioning {

    @Override
    public InputPartition[] planInputPartitions() {
      InputPartition[] partitions = new InputPartition[2];
      partitions[0] = new SpecificInputPartition(new int[]{1, 1, 3}, new int[]{4, 4, 6});
      partitions[1] = new SpecificInputPartition(new int[]{2, 4, 4}, new int[]{6, 2, 2});
      return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new SpecificReaderFactory();
    }

    @Override
    public Partitioning outputPartitioning() {
      Expression[] clustering = new Transform[] { Expressions.identity("i") };
      return new KeyGroupedPartitioning(clustering, 2);
    }
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    return new JavaSimpleBatchTable() {
      @Override
      public Transform[] partitioning() {
        return new Transform[] { Expressions.identity("i") };
      }

      @Override
      public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new MyScanBuilder();
      }
    };
  }

  static class SpecificInputPartition implements InputPartition, HasPartitionKey {
    int[] i;
    int[] j;

    SpecificInputPartition(int[] i, int[] j) {
      assert i.length == j.length;
      this.i = i;
      this.j = j;
    }

    @Override
    public InternalRow partitionKey() {
      return new GenericInternalRow(new Object[] {i[0]});
    }
  }

  static class SpecificReaderFactory implements PartitionReaderFactory {

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      SpecificInputPartition p = (SpecificInputPartition) partition;
      return new PartitionReader<InternalRow>() {
        private int current = -1;

        @Override
        public boolean next() throws IOException {
          current += 1;
          return current < p.i.length;
        }

        @Override
        public InternalRow get() {
          return new GenericInternalRow(new Object[] {p.i[current], p.j[current]});
        }

        @Override
        public void close() throws IOException {

        }
      };
    }
  }
}

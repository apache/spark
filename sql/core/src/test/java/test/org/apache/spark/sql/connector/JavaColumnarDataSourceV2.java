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
import org.apache.spark.sql.connector.TestingV2Source;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;


public class JavaColumnarDataSourceV2 implements TestingV2Source {

  static class MyScanBuilder extends JavaSimpleScanBuilder {

    @Override
    public InputPartition[] planInputPartitions() {
      InputPartition[] partitions = new InputPartition[2];
      partitions[0] = new JavaRangeInputPartition(0, 50);
      partitions[1] = new JavaRangeInputPartition(50, 90);
      return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new ColumnarReaderFactory();
    }
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    return new JavaSimpleBatchTable() {
      @Override
      public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new MyScanBuilder();
      }
    };
  }

  static class ColumnarReaderFactory implements PartitionReaderFactory {
    private static final int BATCH_SIZE = 20;

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return true;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      throw new UnsupportedOperationException("");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      JavaRangeInputPartition p = (JavaRangeInputPartition) partition;
      OnHeapColumnVector i = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
      OnHeapColumnVector j = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
      ColumnVector[] vectors = new ColumnVector[2];
      vectors[0] = i;
      vectors[1] = j;
      ColumnarBatch batch = new ColumnarBatch(vectors);

      return new PartitionReader<ColumnarBatch>() {
        private int current = p.start;

        @Override
        public boolean next() throws IOException {
          i.reset();
          j.reset();
          int count = 0;
          while (current < p.end && count < BATCH_SIZE) {
            i.putInt(count, current);
            j.putInt(count, -current);
            current += 1;
            count += 1;
          }

          if (count == 0) {
            return false;
          } else {
            batch.setNumRows(count);
            return true;
          }
        }

        @Override
        public ColumnarBatch get() {
          return batch;
        }

        @Override
        public void close() throws IOException {
          batch.close();
        }
      };
    }
  }
}

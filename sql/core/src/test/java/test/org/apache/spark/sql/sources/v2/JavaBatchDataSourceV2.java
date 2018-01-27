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

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2Options;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;


public class JavaBatchDataSourceV2 implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceV2Reader, SupportsScanColumnarBatch {
    private final StructType schema = new StructType().add("i", "int").add("j", "int");

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public List<ReadTask<ColumnarBatch>> createBatchReadTasks() {
      return java.util.Arrays.asList(new JavaBatchReadTask(0, 50), new JavaBatchReadTask(50, 90));
    }
  }

  static class JavaBatchReadTask implements ReadTask<ColumnarBatch>, DataReader<ColumnarBatch> {
    private int start;
    private int end;

    private static final int BATCH_SIZE = 20;

    private OnHeapColumnVector i;
    private OnHeapColumnVector j;
    private ColumnarBatch batch;

    JavaBatchReadTask(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public DataReader<ColumnarBatch> createDataReader() {
      this.i = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
      this.j = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
      ColumnVector[] vectors = new ColumnVector[2];
      vectors[0] = i;
      vectors[1] = j;
      this.batch = new ColumnarBatch(vectors);
      return this;
    }

    @Override
    public boolean next() {
      i.reset();
      j.reset();
      int count = 0;
      while (start < end && count < BATCH_SIZE) {
        i.putInt(count, start);
        j.putInt(count, -start);
        start += 1;
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
  }


  @Override
  public DataSourceV2Reader createReader(DataSourceV2Options options) {
    return new Reader();
  }
}

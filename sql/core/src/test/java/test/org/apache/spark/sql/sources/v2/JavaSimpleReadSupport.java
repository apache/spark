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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;

abstract class JavaSimpleReadSupport implements BatchReadSupport {

  @Override
  public StructType fullSchema() {
    return new StructType().add("i", "int").add("j", "int");
  }

  @Override
  public ScanConfigBuilder newScanConfigBuilder() {
    return new JavaNoopScanConfigBuilder(fullSchema());
  }

  @Override
  public PartitionReaderFactory createReaderFactory(ScanConfig config) {
    return new JavaSimpleReaderFactory();
  }
}

class JavaNoopScanConfigBuilder implements ScanConfigBuilder, ScanConfig {

  private StructType schema;

  JavaNoopScanConfigBuilder(StructType schema) {
    this.schema = schema;
  }

  @Override
  public ScanConfig build() {
    return this;
  }

  @Override
  public StructType readSchema() {
    return schema;
  }
}

class JavaSimpleReaderFactory implements PartitionReaderFactory {

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    JavaRangeInputPartition p = (JavaRangeInputPartition) partition;
    return new PartitionReader<InternalRow>() {
      private int current = p.start - 1;

      @Override
      public boolean next() throws IOException {
        current += 1;
        return current < p.end;
      }

      @Override
      public InternalRow get() {
        return new GenericInternalRow(new Object[] {current, -current});
      }

      @Override
      public void close() throws IOException {

      }
    };
  }
}

class JavaRangeInputPartition implements InputPartition {
  int start;
  int end;

  JavaRangeInputPartition(int start, int end) {
    this.start = start;
    this.end = end;
  }
}

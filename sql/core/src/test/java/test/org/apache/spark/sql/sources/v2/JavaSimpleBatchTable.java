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
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.SupportsRead;
import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.sources.v2.TableCapability;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;

abstract class JavaSimpleBatchTable implements Table, SupportsRead {
  private static final Set<TableCapability> CAPABILITIES = new HashSet<>(Arrays.asList(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.TRUNCATE));

  @Override
  public StructType schema() {
    return new StructType().add("i", "int").add("j", "int");
  }

  @Override
  public String name() {
    return this.getClass().toString();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }
}

abstract class JavaSimpleScanBuilder implements ScanBuilder, Scan, Batch {

  @Override
  public Scan build() {
    return this;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public StructType readSchema() {
    return new StructType().add("i", "int").add("j", "int");
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new JavaSimpleReaderFactory();
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


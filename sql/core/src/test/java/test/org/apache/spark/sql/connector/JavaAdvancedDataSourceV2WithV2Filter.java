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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.TestingV2Source;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.filter.Filter;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.expressions.filter.GreaterThan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaAdvancedDataSourceV2WithV2Filter implements TestingV2Source {

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    return new JavaSimpleBatchTable() {
      @Override
      public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new AdvancedScanBuilderWithV2Filter();
      }
    };
  }

  static class AdvancedScanBuilderWithV2Filter implements ScanBuilder, Scan,
    SupportsPushDownV2Filters, SupportsPushDownRequiredColumns {

    private StructType requiredSchema = TestingV2Source.schema();
    private Filter[] filters = new Filter[0];

    @Override
    public void pruneColumns(StructType requiredSchema) {
      this.requiredSchema = requiredSchema;
    }

    @Override
    public StructType readSchema() {
      return requiredSchema;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
      Filter[] supported = Arrays.stream(filters).filter(f -> {
        if (f instanceof GreaterThan) {
          GreaterThan gt = (GreaterThan) f;
          return gt.column().describe().equals("i") && gt.value().value() instanceof Integer;
        } else {
          return false;
        }
      }).toArray(Filter[]::new);

      Filter[] unsupported = Arrays.stream(filters).filter(f -> {
        if (f instanceof GreaterThan) {
          GreaterThan gt = (GreaterThan) f;
          return !gt.column().describe().equals("i") || !(gt.value().value() instanceof Integer);
        } else {
          return true;
        }
      }).toArray(Filter[]::new);

      this.filters = supported;
      return unsupported;
    }

    @Override
    public Filter[] pushedFilters() {
      return filters;
    }

    @Override
    public Scan build() {
      return this;
    }

    @Override
    public Batch toBatch() {
      return new AdvancedBatchWithV2Filter(requiredSchema, filters);
    }
  }

  public static class AdvancedBatchWithV2Filter implements Batch {
    // Exposed for testing.
    public StructType requiredSchema;
    public Filter[] filters;

    AdvancedBatchWithV2Filter(StructType requiredSchema, Filter[] filters) {
      this.requiredSchema = requiredSchema;
      this.filters = filters;
    }

    @Override
    public InputPartition[] planInputPartitions() {
      List<InputPartition> res = new ArrayList<>();

      Integer lowerBound = null;
      for (Filter filter : filters) {
        if (filter instanceof GreaterThan) {
          GreaterThan f = (GreaterThan) filter;
          if ("i".equals(f.column().describe()) && f.value().value() instanceof Integer) {
            lowerBound = (Integer) f.value().value();
            break;
          }
        }
      }

      if (lowerBound == null) {
        res.add(new JavaRangeInputPartition(0, 5));
        res.add(new JavaRangeInputPartition(5, 10));
      } else if (lowerBound < 4) {
        res.add(new JavaRangeInputPartition(lowerBound + 1, 5));
        res.add(new JavaRangeInputPartition(5, 10));
      } else if (lowerBound < 9) {
        res.add(new JavaRangeInputPartition(lowerBound + 1, 10));
      }

      return res.stream().toArray(InputPartition[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new AdvancedReaderFactoryWithV2Filter(requiredSchema);
    }
  }

  static class AdvancedReaderFactoryWithV2Filter implements PartitionReaderFactory {
    StructType requiredSchema;

    AdvancedReaderFactoryWithV2Filter(StructType requiredSchema) {
      this.requiredSchema = requiredSchema;
    }

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
          Object[] values = new Object[requiredSchema.size()];
          for (int i = 0; i < values.length; i++) {
            if ("i".equals(requiredSchema.apply(i).name())) {
              values[i] = current;
            } else if ("j".equals(requiredSchema.apply(i).name())) {
              values[i] = -current;
            }
          }
          return new GenericInternalRow(values);
        }

        @Override
        public void close() throws IOException {

        }
      };
    }
  }
}

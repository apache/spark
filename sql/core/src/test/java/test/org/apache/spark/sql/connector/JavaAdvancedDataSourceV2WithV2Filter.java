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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.TestingV2Source;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

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
    private Predicate[] predicates = new Predicate[0];

    @Override
    public void pruneColumns(StructType requiredSchema) {
      this.requiredSchema = requiredSchema;
    }

    @Override
    public StructType readSchema() {
      return requiredSchema;
    }

    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
      Predicate[] supported = Arrays.stream(predicates).filter(f -> {
        if (f.name().equals(">")) {
          assertInstanceOf(FieldReference.class, f.children()[0]);
          FieldReference column = (FieldReference) f.children()[0];
          assertInstanceOf(LiteralValue.class, f.children()[1]);
          Literal value = (Literal) f.children()[1];
          return column.describe().equals("i") && value.value() instanceof Integer;
        } else {
          return false;
        }
      }).toArray(Predicate[]::new);

      Predicate[] unsupported = Arrays.stream(predicates).filter(f -> {
        if (f.name().equals(">")) {
          assertInstanceOf(FieldReference.class, f.children()[0]);
          FieldReference column = (FieldReference) f.children()[0];
          assertInstanceOf(LiteralValue.class, f.children()[1]);
          Literal value = (LiteralValue) f.children()[1];
          return !column.describe().equals("i") || !(value.value() instanceof Integer);
        } else {
          return true;
        }
      }).toArray(Predicate[]::new);

      this.predicates = supported;
      return unsupported;
    }

    @Override
    public Predicate[] pushedPredicates() {
      return predicates;
    }

    @Override
    public Scan build() {
      return this;
    }

    @Override
    public Batch toBatch() {
      return new AdvancedBatchWithV2Filter(requiredSchema, predicates);
    }
  }

  public static class AdvancedBatchWithV2Filter implements Batch {
    // Exposed for testing.
    public StructType requiredSchema;
    public Predicate[] predicates;

    AdvancedBatchWithV2Filter(StructType requiredSchema, Predicate[] predicates) {
      this.requiredSchema = requiredSchema;
      this.predicates = predicates;
    }

    @Override
    public InputPartition[] planInputPartitions() {
      List<InputPartition> res = new ArrayList<>();

      Integer lowerBound = null;
      for (Predicate predicate : predicates) {
        if (predicate.name().equals(">")) {
          assertInstanceOf(FieldReference.class, predicate.children()[0]);
          FieldReference column = (FieldReference) predicate.children()[0];
          assertInstanceOf(LiteralValue.class, predicate.children()[1]);
          Literal value = (Literal) predicate.children()[1];
          if ("i".equals(column.describe()) && value.value() instanceof Integer integer) {
            lowerBound = integer;
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

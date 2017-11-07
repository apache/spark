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
import java.util.*;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceV2Options;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;

public class JavaAdvancedDataSourceV2 implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceV2Reader, SupportsPushDownRequiredColumns,
      SupportsPushDownFilters {

    private StructType requiredSchema = new StructType().add("i", "int").add("j", "int");
    private Filter[] filters = new Filter[0];

    @Override
    public StructType readSchema() {
      return requiredSchema;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
      this.requiredSchema = requiredSchema;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
      this.filters = filters;
      return new Filter[0];
    }

    @Override
    public Filter[] pushedFilters() {
      return filters;
    }

    @Override
    public List<ReadTask<Row>> createReadTasks() {
      List<ReadTask<Row>> res = new ArrayList<>();

      Integer lowerBound = null;
      for (Filter filter : filters) {
        if (filter instanceof GreaterThan) {
          GreaterThan f = (GreaterThan) filter;
          if ("i".equals(f.attribute()) && f.value() instanceof Integer) {
            lowerBound = (Integer) f.value();
            break;
          }
        }
      }

      if (lowerBound == null) {
        res.add(new JavaAdvancedReadTask(0, 5, requiredSchema));
        res.add(new JavaAdvancedReadTask(5, 10, requiredSchema));
      } else if (lowerBound < 4) {
        res.add(new JavaAdvancedReadTask(lowerBound + 1, 5, requiredSchema));
        res.add(new JavaAdvancedReadTask(5, 10, requiredSchema));
      } else if (lowerBound < 9) {
        res.add(new JavaAdvancedReadTask(lowerBound + 1, 10, requiredSchema));
      }

      return res;
    }
  }

  static class JavaAdvancedReadTask implements ReadTask<Row>, DataReader<Row> {
    private int start;
    private int end;
    private StructType requiredSchema;

    JavaAdvancedReadTask(int start, int end, StructType requiredSchema) {
      this.start = start;
      this.end = end;
      this.requiredSchema = requiredSchema;
    }

    @Override
    public DataReader<Row> createDataReader() {
      return new JavaAdvancedReadTask(start - 1, end, requiredSchema);
    }

    @Override
    public boolean next() {
      start += 1;
      return start < end;
    }

    @Override
    public Row get() {
      Object[] values = new Object[requiredSchema.size()];
      for (int i = 0; i < values.length; i++) {
        if ("i".equals(requiredSchema.apply(i).name())) {
          values[i] = start;
        } else if ("j".equals(requiredSchema.apply(i).name())) {
          values[i] = -start;
        }
      }
      return new GenericRow(values);
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

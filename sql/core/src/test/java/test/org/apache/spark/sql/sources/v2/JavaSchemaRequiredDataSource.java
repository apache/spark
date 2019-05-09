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

import org.apache.spark.sql.sources.v2.Table;
import org.apache.spark.sql.sources.v2.TableProvider;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class JavaSchemaRequiredDataSource implements TableProvider {

  class MyScanBuilder extends JavaSimpleScanBuilder {

    private StructType schema;

    MyScanBuilder(StructType schema) {
      this.schema = schema;
    }

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public InputPartition[] planInputPartitions() {
      return new InputPartition[0];
    }
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options, StructType schema) {
    return new JavaSimpleBatchTable() {

      @Override
      public StructType schema() {
        return schema;
      }

      @Override
      public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new MyScanBuilder(schema);
      }
    };
  }

  @Override
  public Table getTable(CaseInsensitiveStringMap options) {
    throw new IllegalArgumentException("requires a user-supplied schema");
  }
}

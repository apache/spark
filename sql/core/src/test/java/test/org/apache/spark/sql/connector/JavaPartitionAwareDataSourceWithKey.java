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
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.connector.read.InputPartition;

import java.util.Arrays;

public class JavaPartitionAwareDataSourceWithKey extends JavaPartitionAwareDataSource {
  @Override
  protected InputPartition[] getPartitions() {
    return new InputPartition[]{
      new SpecificInputPartitionWithKey(new int[]{1, 1}, new int[]{4, 4}),
      new SpecificInputPartitionWithKey(new int[]{3}, new int[]{6}),
      new SpecificInputPartitionWithKey(new int[]{2}, new int[]{6}),
      new SpecificInputPartitionWithKey(new int[]{4}, new int[]{2}),
      new SpecificInputPartitionWithKey(new int[]{4}, new int[]{2})
    };
  }

  static class SpecificInputPartitionWithKey extends SpecificInputPartition implements HasPartitionKey {
    SpecificInputPartitionWithKey(int[] i, int[] j) {
      super(i, j);
      assert i.length > 0;
      assert Arrays.stream(i).allMatch(k -> k == i[0]);
    }

    @Override
    public InternalRow partitionKey() {
      return new GenericInternalRow(new Object[] {i[0]});
    }
  }
}

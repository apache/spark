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

import com.google.common.collect.Sets;
import org.apache.spark.sql.connector.TestingV2Source;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Set;

public class JavaReportDistinctKeysDataSource implements TestingV2Source {
    static class MyScanBuilder extends JavaSimpleScanBuilder implements SupportsReportDistinctKeys {
        @Override
        public Set<Set<NamedReference>> distinctKeysSet() {
            return Sets.newHashSet(
                    Sets.newHashSet(FieldReference.apply("i")),
                    Sets.newHashSet(FieldReference.apply("j")));
        }

        @Override
        public InputPartition[] planInputPartitions() {
            InputPartition[] partitions = new InputPartition[1];
            partitions[0] = new JavaRangeInputPartition(0, 1);
            return partitions;
        }
    }

    @Override
    public Table getTable(CaseInsensitiveStringMap options) {
        return new JavaSimpleBatchTable() {
            @Override
            public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
                return new JavaReportDistinctKeysDataSource.MyScanBuilder();
            }
        };
    }
}

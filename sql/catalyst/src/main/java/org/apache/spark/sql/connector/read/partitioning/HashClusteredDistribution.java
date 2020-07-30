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

package org.apache.spark.sql.connector.read.partitioning;

import org.apache.spark.annotation.Evolving;

import java.util.OptionalInt;

/**
 * A concrete implementation of {@link Distribution}.
 * Represents data where tuples have been clustered according to the hash of the given columns.
 *
 * This is a strictly stronger guarantee than [[ClusteredDistribution]]. Given a tuple and the
 * number of partitions, this distribution strictly requires which partition the tuple should be in.
 *
 * @since 3.x.x
 */
@Evolving
public class HashClusteredDistribution implements Distribution {
    public final String[] clusteredColumns;
    public final OptionalInt numPartitions;

    public HashClusteredDistribution(String[] clusteredColumns, OptionalInt numPartitions) {
        this.clusteredColumns = clusteredColumns;
        this.numPartitions = numPartitions;
    }
}

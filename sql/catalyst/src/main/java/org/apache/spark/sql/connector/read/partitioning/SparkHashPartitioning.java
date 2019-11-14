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

import java.util.Arrays;
import java.util.List;

/**
 * A concrete implementation of {@link Partitioning}. Represents a partitioning where rows are
 * split up across partitions based on the spark defined hash of columns. that
 * share the same values for the {@link #clusteredColumns} will be produced by the same
 * {@link org.apache.spark.sql.connector.read.PartitionReader} and that the index will be
 * consistent.
 */
@Evolving
public class SparkHashPartitioning implements Partitioning {
    /**
     * The names of the clustered columns. Note that they are order insensitive.
     */
    public final List<String> clusteredColumns;

    public final int numberOfPartitions;

    public SparkHashPartitioning(String[] clusteredColumns, int numberOfPartitions) {
        this.clusteredColumns = Arrays.asList(clusteredColumns);
        this.numberOfPartitions = numberOfPartitions;
    }

    @Override
    public int numPartitions() {
        return numberOfPartitions;
    }

    @Override
    public boolean satisfy(Distribution distribution) {
        if(distribution instanceof SparkHashClusteredDistribution){
            SparkHashClusteredDistribution hashDist = (SparkHashClusteredDistribution)distribution;
            if(hashDist.numberOfPartitions == numberOfPartitions
            && Arrays.stream(hashDist.clusteredColumns).allMatch(c -> clusteredColumns.contains(c))
            && hashDist.clusteredColumns.length == clusteredColumns.size()){
                return true;
            }
        }
        if(distribution instanceof ClusteredDistribution){
            ClusteredDistribution clustDist = (ClusteredDistribution)distribution;
            List<String> clustCols = Arrays.asList(clustDist.clusteredColumns);
            if(clusteredColumns.stream().allMatch(col -> clustCols.contains(col))){
                return true;
            }
        }
        return false;
    }
}

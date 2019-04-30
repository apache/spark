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

package org.apache.spark.sql.sources.v2.reader.partitioning;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.sources.v2.reader.PartitionReader;

/**
 * An interface to represent data distribution requirement, which specifies how the records should
 * be distributed among the data partitions (one {@link PartitionReader} outputs data for one
 * partition).
 * Note that this interface has nothing to do with the data ordering inside one
 * partition(the output records of a single {@link PartitionReader}).
 *
 * The instance of this interface is created and provided by Spark, then consumed by
 * {@link Partitioning#satisfy(Distribution)}. This means data source developers don't need to
 * implement this interface, but need to catch as more concrete implementations of this interface
 * as possible in {@link Partitioning#satisfy(Distribution)}.
 *
 * Concrete implementations until now:
 * <ul>
 *   <li>{@link ClusteredDistribution}</li>
 * </ul>
 */
@Evolving
public interface Distribution {}

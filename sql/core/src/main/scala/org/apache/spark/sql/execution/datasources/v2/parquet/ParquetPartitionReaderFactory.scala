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
package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A factory used to create Parquet readers.
 *
 * @param sqlConf         SQL configuration.
 * @param broadcastedConf Broadcast serializable Hadoop Configuration.
 * @param dataSchema      Schema of Parquet files.
 * @param readDataSchema  Required schema of Parquet files.
 * @param partitionSchema Schema of partitions.
 * @param filters         Filters to be pushed down in the batch scan.
 * @param aggregation     Aggregation to be pushed down in the batch scan.
 * @param options         The options of Parquet datasource that are set for the read.
 */
case class ParquetPartitionReaderFactory(
                                          sqlConf: SQLConf,
                                          broadcastedConf: Broadcast[SerializableConfiguration],
                                          dataSchema: StructType,
                                          readDataSchema: StructType,
                                          partitionSchema: StructType,
                                          filters: Array[Filter],
                                          aggregation: Option[Aggregation],
                                          options: ParquetOptions
                                        ) extends ParquetPartitionReaderFactoryBase(
  sqlConf, broadcastedConf, dataSchema, readDataSchema,
  partitionSchema, filters, aggregation, options
)

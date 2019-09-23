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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.Evolving
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, PartitionReaderFactory}
import org.apache.spark.util.SerializableConfiguration

/**
 * A helper trait to serialize and broadcast the Hadoop configuration for readers.
 */
@Evolving
trait BroadcastedHadoopConfBatch extends Batch {
  val sparkSession: SparkSession;
  val conf: Configuration;
  // Override this if you need to set custom keys on the Hadoop configuration
  def updateHadoopConf(conf: Configuration): Configuration = {
    conf
  }

  override final def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(updateHadoopConf(conf)))
    createReaderFactory(broadcastedConf)
  }

  def createReaderFactory(broadcastedConf: Broadcast[SerializableConfiguration]): PartitionReaderFactory
}

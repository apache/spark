/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.remoteshuffle.messages.MessageConstants

object RssOpts {
  val maxServerCount: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.maxServerCount")
      .doc("max remote shuffle servers used for this application.")
      .intConf
      .createWithDefault(50)
  val minServerCount: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.minServerCount")
      .doc("min remote shuffle servers used for this application.")
      .intConf
      .createWithDefault(1)
  val serverRatio: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.serverRatio")
      .doc("how many executors mapping to one shuffle server.")
      .intConf
      .createWithDefault(20)
  val writerQueueSize: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.queueSize")
      .doc("writer queue size for shuffle writer to store shuffle records and send them to " +
        "shuffle server in background threads.")
      .intConf
      .createWithDefault(0)
  val writerMaxThreads: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.maxThreads")
      .doc("max number of threads for shuffle writer to store shuffle records and send them " +
        "to shuffle server in background threads.")
      .intConf
      .createWithDefault(2)
  val writerAsyncFinish: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.writer.asyncFinish")
      .doc("whether use async mode for writer to finish uploading data.")
      .booleanConf
      .createWithDefault(false)
  val writerBufferSize: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.bufferSize")
      .doc("Internal buffer size for shuffle writer.")
      .intConf
      .createWithDefault(MessageConstants.DEFAULT_SHUFFLE_DATA_MESSAGE_SIZE)
  val writerBufferMax: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.bufferMax")
      .doc("Internal buffer max limit for shuffle writer. The buffer could temporarily " +
        "grow to this size if there " +
        "is a single large shuffle record to serialize into the buffer.")
      .intConf
      .createWithDefault(512 * 1024 * 1024)
  val writerBufferSpill: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.writer.bufferSpill")
      .doc("The threshold for shuffle writer buffer to spill data. Here spill means removing " +
        "the data out of the buffer " +
        "and send to shuffle server.")
      .intConf
      .createWithDefault(32 * 1024 * 1024)
  val writerSupportAggregate: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.writer.supportAggregate")
      .doc("Whether using the new writer buffer implementation which supports map side " +
        "aggregation")
      .booleanConf
      .createWithDefault(false)
  val networkTimeout: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.networkTimeout")
      .doc("network timeout (milliseconds) for shuffle client.")
      .longConf
      .createWithDefault(4 * 60 * 1000L)
  val useConnectionPool: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.useConnectionPool")
      .doc("use connection pool for shuffle client.")
      .booleanConf
      .createWithDefault(false)
  val networkRetries: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.networkRetries")
      .doc("max network retries when retrying is supported, e.g. connecting to zookeeper.")
      .intConf
      .createWithDefault(5)
  val maxWaitTime: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.maxWaitTime")
      .doc("maximum wait time (milliseconds) for shuffle client, e.g. retry connecting to " +
        "busy remote shuffle server.")
      .longConf
      .createWithDefault(3 * 60 * 1000L)
  val pollInterval: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.pollInterval")
      .doc("poll interval (milliseconds) to query remote shuffle server for status update, " +
        "e.g. whether a map task's data flushed.")
      .intConf
      .createWithDefault(200)
  val readerDataAvailableWaitTime: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.reader.dataAvailableWaitTime")
      .doc("max wait time in shuffle reader to wait data ready in the shuffle server.")
      .longConf
      .createWithDefault(5 * 60 * 1000L)
  val readerSorterBufferSize: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.reader.sorterBufferSize")
      .doc("buffer size for the sorter used in shuffle reader")
      .stringConf
      .createWithDefault("32k")
  val readerSorterMemoryThreshold: ConfigEntry[Long] =
    ConfigBuilder("spark.shuffle.rss.reader.sorterMemoryThreshold")
      .doc("memory threshold for the sorter used in shuffle reader")
      .longConf
      .createWithDefault(20 * 1024 * 1024)
  val dataCenter: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.dataCenter")
      .doc("data center for RSS cluster")
      .stringConf
      .createWithDefault("dataCenter1")
  val cluster: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.cluster")
      .doc("RSS cluster name.")
      .stringConf
      .createWithDefault("default")
  val serviceRegistryType: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serviceRegistry.type")
      .doc("type of service registry to use: zookeeper, standalone.")
      .stringConf
      .createWithDefault("zookeeper")
  val serviceRegistryServer: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serviceRegistry.server")
      .doc("Registry server host:port addresses.")
      .stringConf
      .createWithDefault("")
  val serverSequenceServerId: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serverSequence.serverId")
      .doc("Server ID format for server sequence")
      .stringConf
      .createWithDefault("rss-%s")
  val serverSequenceConnectionString: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.serverSequence.connectionString")
      .doc("Connection string format for server sequence")
      .stringConf
      .createWithDefault("rss-%s.rss.remote-shuffle-service.svc.cluster.local:9338")
  val serverSequenceStartIndex: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.serverSequence.startIndex")
      .doc("Server sequence start index")
      .intConf
      .createWithDefault(0)
  val serverSequenceEndIndex: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.serverSequence.endIndex")
      .doc("Server sequence start index")
      .intConf
      .createWithDefault(0)
  val minSplits: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.minSplits")
      .doc("min number of splits for each shuffle partition on each shuffle server.")
      .intConf
      .createWithDefault(1)
  val maxSplits: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.maxSplits")
      .doc("max number of splits for each shuffle partition on each shuffle server.")
      .intConf
      .createWithDefault(1)
  val mapsPerSplit: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.mapsPerSplit")
      .doc("how many map tasks write to same shuffle partition split. Large value here will " +
        "have more memory consumption because RSS client needs to maintain internal memory " +
        "buffer for each task.")
      .intConf
      .createWithDefault(800)
  val replicas: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.rss.replicas")
      .doc("number of replicas for replicated shuffle client.")
      .intConf
      .createWithDefault(1)
  val checkReplicaConsistency: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.rss.checkReplicaConsistency")
      .doc("Check replica data consistency when reading replicas. If set to true, it will " +
        "consume more memory to track the data in client side.")
      .booleanConf
      .createWithDefault(false)
  val excludeHosts: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.rss.excludeHosts")
      .doc("the server hosts to exclude, separated by comma.")
      .stringConf
      .createWithDefault("")
}

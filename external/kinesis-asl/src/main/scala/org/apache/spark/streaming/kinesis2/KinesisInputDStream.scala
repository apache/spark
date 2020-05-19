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

package org.apache.spark.streaming.kinesis2

import java.net.URI

import org.apache.hadoop.classification.InterfaceStability
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.kinesis.common.InitialPositionInStream
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.reflect.ClassTag

private[kinesis2] class KinesisInputDStream[T: ClassTag](_ssc: StreamingContext,
                                                         val streamName: String,
                                                         val endpointUrl: URI,
                                                         val regionName: String,
                                                         val kinesisCreds: SparkAWSCredentials,
                                                         val dynamoDBCreds: Option[SparkAWSCredentials],
                                                         val cloudWatchCreds: Option[SparkAWSCredentials],
                                                         val cloudWatchUrl: Option[URI],
                                                         val checkpointAppName: String,
                                                         val checkpointInterval: Duration,
                                                         val maxRecords: Option[Integer],
                                                         val protocol: Option[Protocol],
                                                         val initialPositionInStream: Option[InitialPositionInStream],
                                                         val dynamoProxyHost: Option[String],
                                                         val dynamoProxyPort: Option[Integer],
                                                         val _storageLevel: StorageLevel,
                                                         val messageHandler: KinesisClientRecord => T
                                                        ) extends ReceiverInputDStream[T](_ssc) {

  private[streaming]
  override def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] = {

    // This returns true even for when blockInfos is empty
    val allBlocksHaveRanges = blockInfos.map {
      _.metadataOption
    }.forall(_.nonEmpty)

    if (allBlocksHaveRanges) {
      // Create a KinesisBackedBlockRDD, even when there are no blocks
      val blockIds = blockInfos.map {
        _.blockId.asInstanceOf[BlockId]
      }.toArray
      val seqNumRanges = blockInfos.map {
        _.metadataOption.get.asInstanceOf[SequenceNumberRanges]
      }.toArray
      val isBlockIdValid = blockInfos.map {
        _.isBlockIdValid()
      }.toArray
      logDebug(s"Creating KinesisBackedBlockRDD for $time with ${seqNumRanges.length} " +
        s"seq number ranges: ${seqNumRanges.mkString(", ")} ")

      new KinesisBackedBlockRDD(
        context.sc, regionName, endpointUrl, blockIds, seqNumRanges,
        isBlockIdValid = isBlockIdValid,
        messageHandler = messageHandler,
        kinesisCreds = kinesisCreds,
        kinesisReadConfigs = KinesisReadConfigurations(ssc))
    } else {
      logWarning("Kinesis sequence number information was not present with some block metadata," +
        " it may not be possible to recover from failures")
      super.createBlockRDD(time, blockInfos)
    }
  }

  override def getReceiver(): Receiver[T] = {
    new KinesisReceiver(streamName, endpointUrl, regionName, kinesisCreds, dynamoDBCreds, cloudWatchCreds, cloudWatchUrl, checkpointAppName,
      checkpointInterval, initialPositionInStream, maxRecords, protocol, dynamoProxyHost, dynamoProxyPort, _storageLevel, messageHandler)
  }
}

@InterfaceStability.Evolving
object KinesisInputDStream {

  /**
   * Builder for [[KinesisInputDStream]] instances.
   *
   * @since 2.2.0
   */
  @InterfaceStability.Evolving
  class Builder {
    // Required params
    private var streamingContext: Option[StreamingContext] = None
    private var streamName: Option[String] = None
    private var checkpointAppName: Option[String] = None

    // Params with defaults
    private var endpointUrl: Option[URI] = None
    private var regionName: Option[String] = None
    private var initialPosition: Option[InitialPositionInStream] = None
    private var checkpointInterval: Option[Duration] = None
    private var storageLevel: Option[StorageLevel] = None
    private var kinesisCreds: Option[SparkAWSCredentials] = None
    private var dynamoDBCreds: Option[SparkAWSCredentials] = None
    private var cloudWatchCreds: Option[SparkAWSCredentials] = None
    private var dynamoProxyHost: Option[String] = None
    private var dynamoProxyPort: Option[Integer] = None
    private var maxRecords: Option[Integer] = None
    private var protocol: Option[Protocol] = None
    private var cloudWatchUrl: Option[URI] = None

    /**
     * Sets the StreamingContext that will be used to construct the Kinesis DStream. This is a
     * required parameter.
     *
     * @param ssc [[StreamingContext]] used to construct Kinesis DStreams
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def streamingContext(ssc: StreamingContext): Builder = {
      streamingContext = Option(ssc)
      this
    }

    /**
     * Sets the StreamingContext that will be used to construct the Kinesis DStream. This is a
     * required parameter.
     *
     * @param jssc [[JavaStreamingContext]] used to construct Kinesis DStreams
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def streamingContext(jssc: JavaStreamingContext): Builder = {
      streamingContext = Option(jssc.ssc)
      this
    }

    /**
     * Sets the name of the Kinesis stream that the DStream will read from. This is a required
     * parameter.
     *
     * @param streamName Name of Kinesis stream that the DStream will read from
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def streamName(streamName: String): Builder = {
      this.streamName = Option(streamName)
      this
    }

    /**
     * Sets the KCL application name to use when checkpointing state to DynamoDB. This is a
     * required parameter.
     *
     * @param appName Value to use for the KCL app name (used when creating the DynamoDB checkpoint
     *                table and when writing metrics to CloudWatch)
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def checkpointAppName(appName: String): Builder = {
      checkpointAppName = Option(appName)
      this
    }

    /**
     * Sets the AWS Kinesis endpoint URL. Defaults to "https://kinesis.us-east-1.amazonaws.com" if
     * no custom value is specified
     *
     * @param url Kinesis endpoint URL to use
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def endpointUrl(url: URI): Builder = {
      endpointUrl = Option(url)
      this
    }

    /**
     * Sets the AWS region to construct clients for. Defaults to "us-east-1" if no custom value
     * is specified.
     *
     * @param regionName Name of AWS region to use (e.g. "us-west-2")
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def regionName(regionName: String): Builder = {
      this.regionName = Option(regionName)
      this
    }

    /**
     * Sets how often the KCL application state is checkpointed to DynamoDB. Defaults to the Spark
     * Streaming batch interval if no custom value is specified.
     *
     * @param interval [[Duration]] specifying how often the KCL state should be checkpointed to
     *                 DynamoDB.
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def checkpointInterval(interval: Duration): Builder = {
      this.checkpointInterval = Option(interval)
      this
    }

    /**
     * Sets the initial position data is read from in the Kinesis stream. Defaults to
     * [[InitialPositionInStream.LATEST]] if no custom value is specified.
     * This function would be removed when we deprecate the KinesisUtils.
     *
     * @param initialPosition InitialPositionInStream value specifying where Spark Streaming
     *                        will start reading records in the Kinesis stream from
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def initialPositionInStream(initialPosition: InitialPositionInStream): Builder = {
      this.initialPosition = Option(initialPosition)
      this
    }

    /**
     * Sets the storage level of the blocks for the DStream created. Defaults to
     * [[StorageLevel.MEMORY_AND_DISK_2]] if no custom value is specified.
     *
     * @param storageLevel [[StorageLevel]] to use for the DStream data blocks
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def storageLevel(storageLevel: StorageLevel): Builder = {
      this.storageLevel = Option(storageLevel)
      this
    }

    /**
     *
     * @param kinesisCreds
     * @return
     */
    def kinesisCreds(kinesisCreds: SparkAWSCredentials): Builder = {
      this.kinesisCreds = Option(kinesisCreds)
      this
    }

    /**
     *
     * @param cloudWatchCreds
     * @return
     */
    def cloudWatchCreds(cloudWatchCreds: SparkAWSCredentials): Builder = {
      this.cloudWatchCreds = Option(cloudWatchCreds)
      this
    }

    /**
     *
     * @param cloudWatchUrl
     * @return
     */
    def cloudWatchUrl(cloudWatchUrl: URI): Builder = {
      this.cloudWatchUrl = Option(cloudWatchUrl)
      this
    }

    /**
     *
     * @param dynamoDBCreds
     * @return
     */
    def dynamoDBCreds(dynamoDBCreds: SparkAWSCredentials): Builder = {
      this.dynamoDBCreds = Option(dynamoDBCreds)
      this
    }

    /**
     *
     * @param dynamoProxyHost
     * @return
     */
    def dynamoProxyHost(dynamoProxyHost: String): Builder = {
      this.dynamoProxyHost = Option(dynamoProxyHost)
      this
    }

    /**
     *
     * @param dynamoProxyPort
     * @return
     */
    def dynamoProxyPort(dynamoProxyPort: Integer): Builder = {
      this.dynamoProxyPort = Option(dynamoProxyPort)
      this
    }

    /**
     *
     * @param maxRecords
     * @return
     */
    def maxRecords(maxRecords: Integer): Builder = {
      this.maxRecords = Option(maxRecords)
      this
    }

    /**
     *
     * @param protocol
     * @return
     */
    def protocol(protocol: Protocol): Builder = {
      this.protocol = Option(protocol)
      this
    }

    /**
     * Create a new instance of [[KinesisInputDStream]] with configured parameters and the provided
     * message handler.
     *
     * @param handler Function converting [[Record]] instances read by the KCL to DStream type [[T]]
     * @return Instance of [[KinesisInputDStream]] constructed with configured parameters
     */
    def buildWithMessageHandler[T: ClassTag](handler: KinesisClientRecord => T): KinesisInputDStream[T] = {
      val ssc = getRequiredParam(streamingContext, "streamingContext")
      val credentials = kinesisCreds.getOrElse(null)
      new KinesisInputDStream(
        ssc,
        getRequiredParam(streamName, "streamName"),
        endpointUrl.getOrElse(DEFAULT_KINESIS_ENDPOINT_URL),
        regionName.getOrElse(DEFAULT_KINESIS_REGION_NAME),
        credentials,
        Option(dynamoDBCreds.getOrElse(credentials)),
        Option(cloudWatchCreds.getOrElse(credentials)),
        Option(cloudWatchUrl.getOrElse(DEFAULT_MONITORING_ENDPOINT_URL)),
        getRequiredParam(checkpointAppName, "checkpointAppName"),
        checkpointInterval.getOrElse(ssc.graph.batchDuration),
        Option(maxRecords.getOrElse(100:Integer)),
        Option(protocol.getOrElse(Protocol.HTTP1_1)),
        Option(initialPosition.getOrElse(INITIAL_POSITION_INSTREAM)),
        Option(dynamoProxyHost.getOrElse("")),
        Option(dynamoProxyPort.getOrElse(0)),
        storageLevel.getOrElse(DEFAULT_STORAGE_LEVEL),
        ssc.sc.clean(handler))
    }

    /**
     * Create a new instance of [[KinesisInputDStream]] with configured parameters and using the
     * default message handler, which returns [[Array[Byte]]].
     *
     * @return Instance of [[KinesisInputDStream]] constructed with configured parameters
     */
    def build(): KinesisInputDStream[Array[Byte]] = buildWithMessageHandler(defaultMessageHandler)

    private def getRequiredParam[T](param: Option[T], paramName: String): T = param.getOrElse {
      throw new IllegalArgumentException(s"No value provided for required parameter $paramName")
    }
  }

  /**
   * Creates a [[KinesisInputDStream.Builder]] for constructing [[KinesisInputDStream]] instances.
   *
   * @since 2.2.0
   * @return [[KinesisInputDStream.Builder]] instance
   */
  def builder: Builder = new Builder

  private[kinesis2] def defaultMessageHandler(record: KinesisClientRecord): Array[Byte] = {
    if (record == null) return null
    val byteBuffer = record.data()
    val byteArray = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(byteArray)
    byteArray
  }

  private[kinesis2] val DEFAULT_KINESIS_ENDPOINT_URL: URI = new URI("https://kinesis.us-east-1.amazonaws.com")
  private[kinesis2] val DEFAULT_MONITORING_ENDPOINT_URL: URI = new URI("https://monitoring.us-east-1.amazonaws.com")
  private[kinesis2] val DEFAULT_KINESIS_REGION_NAME: String = Region.US_EAST_1.toString
  private[kinesis2] val DEFAULT_STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK_2
  private[kinesis2] val INITIAL_POSITION_INSTREAM: InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON
}


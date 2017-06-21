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

package org.apache.spark.streaming.kinesis

import scala.reflect.ClassTag

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.Record
import KinesisReadConfigurations._

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo

private[kinesis] class KinesisInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    val streamName: String,
    val endpointUrl: String,
    val regionName: String,
    val initialPositionInStream: InitialPositionInStream,
    val checkpointAppName: String,
    val checkpointInterval: Duration,
    val _storageLevel: StorageLevel,
    val messageHandler: Record => T,
    val kinesisCreds: SparkAWSCredentials,
    val dynamoDBCreds: Option[SparkAWSCredentials],
    val cloudWatchCreds: Option[SparkAWSCredentials]
  ) extends ReceiverInputDStream[T](_ssc) {

  private[streaming]
  override def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] = {

    // This returns true even for when blockInfos is empty
    val allBlocksHaveRanges = blockInfos.map { _.metadataOption }.forall(_.nonEmpty)

    if (allBlocksHaveRanges) {
      // Create a KinesisBackedBlockRDD, even when there are no blocks
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray
      val seqNumRanges = blockInfos.map {
        _.metadataOption.get.asInstanceOf[SequenceNumberRanges] }.toArray
      val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
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
    new KinesisReceiver(streamName, endpointUrl, regionName, initialPositionInStream,
      checkpointAppName, checkpointInterval, _storageLevel, messageHandler,
      kinesisCreds, dynamoDBCreds, cloudWatchCreds)
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
    private var endpointUrl: Option[String] = None
    private var regionName: Option[String] = None
    private var initialPositionInStream: Option[InitialPositionInStream] = None
    private var checkpointInterval: Option[Duration] = None
    private var storageLevel: Option[StorageLevel] = None
    private var kinesisCredsProvider: Option[SparkAWSCredentials] = None
    private var dynamoDBCredsProvider: Option[SparkAWSCredentials] = None
    private var cloudWatchCredsProvider: Option[SparkAWSCredentials] = None

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
    def endpointUrl(url: String): Builder = {
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
     * Sets the initial position data is read from in the Kinesis stream. Defaults to
     * [[InitialPositionInStream.LATEST]] if no custom value is specified.
     *
     * @param initialPosition InitialPositionInStream value specifying where Spark Streaming
     *                        will start reading records in the Kinesis stream from
     * @return Reference to this [[KinesisInputDStream.Builder]]
     */
    def initialPositionInStream(initialPosition: InitialPositionInStream): Builder = {
      initialPositionInStream = Option(initialPosition)
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
      checkpointInterval = Option(interval)
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
     * Sets the [[SparkAWSCredentials]] to use for authenticating to the AWS Kinesis
     * endpoint. Defaults to [[DefaultCredentialsProvider]] if no custom value is specified.
     *
     * @param credentials [[SparkAWSCredentials]] to use for Kinesis authentication
     */
    def kinesisCredentials(credentials: SparkAWSCredentials): Builder = {
      kinesisCredsProvider = Option(credentials)
      this
    }

    /**
     * Sets the [[SparkAWSCredentials]] to use for authenticating to the AWS DynamoDB
     * endpoint. Will use the same credentials used for AWS Kinesis if no custom value is set.
     *
     * @param credentials [[SparkAWSCredentials]] to use for DynamoDB authentication
     */
    def dynamoDBCredentials(credentials: SparkAWSCredentials): Builder = {
      dynamoDBCredsProvider = Option(credentials)
      this
    }

    /**
     * Sets the [[SparkAWSCredentials]] to use for authenticating to the AWS CloudWatch
     * endpoint. Will use the same credentials used for AWS Kinesis if no custom value is set.
     *
     * @param credentials [[SparkAWSCredentials]] to use for CloudWatch authentication
     */
    def cloudWatchCredentials(credentials: SparkAWSCredentials): Builder = {
      cloudWatchCredsProvider = Option(credentials)
      this
    }

    /**
     * Create a new instance of [[KinesisInputDStream]] with configured parameters and the provided
     * message handler.
     *
     * @param handler Function converting [[Record]] instances read by the KCL to DStream type [[T]]
     * @return Instance of [[KinesisInputDStream]] constructed with configured parameters
     */
    def buildWithMessageHandler[T: ClassTag](
        handler: Record => T): KinesisInputDStream[T] = {
      val ssc = getRequiredParam(streamingContext, "streamingContext")
      new KinesisInputDStream(
        ssc,
        getRequiredParam(streamName, "streamName"),
        endpointUrl.getOrElse(DEFAULT_KINESIS_ENDPOINT_URL),
        regionName.getOrElse(DEFAULT_KINESIS_REGION_NAME),
        initialPositionInStream.getOrElse(DEFAULT_INITIAL_POSITION_IN_STREAM),
        getRequiredParam(checkpointAppName, "checkpointAppName"),
        checkpointInterval.getOrElse(ssc.graph.batchDuration),
        storageLevel.getOrElse(DEFAULT_STORAGE_LEVEL),
        ssc.sc.clean(handler),
        kinesisCredsProvider.getOrElse(DefaultCredentials),
        dynamoDBCredsProvider,
        cloudWatchCredsProvider)
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
   *
   * @return [[KinesisInputDStream.Builder]] instance
   */
  def builder: Builder = new Builder

  private[kinesis] def defaultMessageHandler(record: Record): Array[Byte] = {
    if (record == null) return null
    val byteBuffer = record.getData()
    val byteArray = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(byteArray)
    byteArray
  }

  private[kinesis] val DEFAULT_KINESIS_ENDPOINT_URL: String =
    "https://kinesis.us-east-1.amazonaws.com"
  private[kinesis] val DEFAULT_KINESIS_REGION_NAME: String = "us-east-1"
  private[kinesis] val DEFAULT_INITIAL_POSITION_IN_STREAM: InitialPositionInStream =
    InitialPositionInStream.LATEST
  private[kinesis] val DEFAULT_STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK_2
}

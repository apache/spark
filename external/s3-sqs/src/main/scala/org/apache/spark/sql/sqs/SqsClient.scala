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

package org.apache.spark.sql.sqs

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.amazonaws.{AmazonClientException, AmazonServiceException, ClientConfiguration}
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.{DeleteMessageBatchRequestEntry, Message, ReceiveMessageRequest}
import org.apache.hadoop.conf.Configuration
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils

class SqsClient(sourceOptions: SqsSourceOptions,
                hadoopConf: Configuration) extends Logging {

  private val sqsFetchIntervalSeconds = sourceOptions.fetchIntervalSeconds
  private val sqsLongPollWaitTimeSeconds = sourceOptions.longPollWaitTimeSeconds
  private val sqsMaxRetries = sourceOptions.maxRetries
  private val maxConnections = sourceOptions.maxConnections
  private val ignoreFileDeletion = sourceOptions.ignoreFileDeletion
  private val region = sourceOptions.region
  val sqsUrl = sourceOptions.sqsUrl

  @volatile var exception: Option[Exception] = None

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
  private var retriesOnFailure = 0
  private val sqsClient = createSqsClient()

  val sqsScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("sqs-scheduler")

  val sqsFileCache = new SqsFileCache(sourceOptions.maxFileAgeMs, sourceOptions.fileNameOnly)

  val deleteMessageQueue = new java.util.concurrent.ConcurrentLinkedQueue[String]()

  private val sqsFetchMessagesThread = new Runnable {
    override def run(): Unit = {
      try {
        // Fetching messages from Amazon SQS
        val newMessages = sqsFetchMessages()

        // Filtering the new messages which are already not seen
        if (newMessages.nonEmpty) {
          newMessages.filter(message => sqsFileCache.isNewFile(message._1, message._2))
            .foreach(message =>
              sqsFileCache.add(message._1, MessageDescription(message._2, false, message._3)))
        }
      } catch {
        case e: Exception =>
          exception = Some(e)
      }
    }
  }

  sqsScheduler.scheduleWithFixedDelay(
    sqsFetchMessagesThread,
    0,
    sqsFetchIntervalSeconds,
    TimeUnit.SECONDS)

  private def sqsFetchMessages(): Seq[(String, Long, String)] = {
    val messageList = try {
      val receiveMessageRequest = new ReceiveMessageRequest()
        .withQueueUrl(sqsUrl)
        .withWaitTimeSeconds(sqsLongPollWaitTimeSeconds)
      val messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages.asScala
      retriesOnFailure = 0
      logDebug(s"successfully received ${messages.size} messages")
      messages
    } catch {
      case ase: AmazonServiceException =>
        val message =
        """
          |Caught an AmazonServiceException, which means your request made it to Amazon SQS,
          | rejected with an error response for some reason.
        """.stripMargin
        logWarning(message)
        logWarning(s"Error Message: ${ase.getMessage}")
        logWarning(s"HTTP Status Code: ${ase.getStatusCode}, AWS Error Code: ${ase.getErrorCode}")
        logWarning(s"Error Type: ${ase.getErrorType}, Request ID: ${ase.getRequestId}")
        evaluateRetries()
        List.empty
      case ace: AmazonClientException =>
        val message =
        """
           |Caught an AmazonClientException, which means, the client encountered a serious
           | internal problem while trying to communicate with Amazon SQS, such as not
           |  being able to access the network.
        """.stripMargin
        logWarning(message)
        logWarning(s"Error Message: ${ace.getMessage()}")
        evaluateRetries()
        List.empty
      case e: Exception =>
        val message = "Received unexpected error from SQS"
        logWarning(message)
        logWarning(s"Error Message: ${e.getMessage()}")
        evaluateRetries()
        List.empty
    }
    if (messageList.nonEmpty) {
      parseSqsMessages(messageList)
    } else {
      Seq.empty
    }
  }

  private def parseSqsMessages(messageList: Seq[Message]): Seq[(String, Long, String)] = {
    val errorMessages = scala.collection.mutable.ListBuffer[String]()
    val parsedMessages = messageList.foldLeft(Seq[(String, Long, String)]()) { (list, message) =>
      implicit val formats = DefaultFormats
      try {
        val messageReceiptHandle = message.getReceiptHandle
        val messageJson = parse(message.getBody).extract[JValue]
        val bucketName = (
          messageJson \ "Records" \ "s3" \ "bucket" \ "name").extract[Array[String]].head
        val eventName = (messageJson \ "Records" \ "eventName").extract[Array[String]].head
        if (eventName.contains("ObjectCreated")) {
          val timestamp = (messageJson \ "Records" \ "eventTime").extract[Array[String]].head
          val timestampMills = convertTimestampToMills(timestamp)
          val path = "s3://" +
            bucketName + "/" +
            (messageJson \ "Records" \ "s3" \ "object" \ "key").extract[Array[String]].head
          logDebug("Successfully parsed sqs message")
          list :+ ((path, timestampMills, messageReceiptHandle))
        } else {
          if (eventName.contains("ObjectRemoved")) {
            if (!ignoreFileDeletion) {
              exception = Some(new SparkException("ObjectDelete message detected in SQS"))
            } else {
              logInfo("Ignoring file deletion message since ignoreFileDeletion is true")
            }
          } else {
            logWarning("Ignoring unexpected message detected in SQS")
          }
          errorMessages.append(messageReceiptHandle)
          list
        }
      } catch {
        case me: MappingException =>
          errorMessages.append(message.getReceiptHandle)
          logWarning(s"Error in parsing SQS message ${me.getMessage}")
          list
        case e: Exception =>
          errorMessages.append(message.getReceiptHandle)
          logWarning(s"Unexpected error while parsing SQS message ${e.getMessage}")
          list
      }
    }
    if (errorMessages.nonEmpty) {
      addToDeleteMessageQueue(errorMessages.toList)
    }
    parsedMessages
  }

  private def convertTimestampToMills(timestamp: String): Long = {
    val timeInMillis = timestampFormat.parse(timestamp).getTime()
    timeInMillis
  }

  private def evaluateRetries(): Unit = {
    retriesOnFailure += 1
    if (retriesOnFailure >= sqsMaxRetries) {
      logError("Max retries reached")
      exception = Some(new SparkException("Unable to receive Messages from SQS for " +
        s"${sqsMaxRetries} times Giving up. Check logs for details."))
    } else {
      logWarning(s"Attempt ${retriesOnFailure}." +
        s"Will reattempt after ${sqsFetchIntervalSeconds} seconds")
    }
  }

  private def createSqsClient(): AmazonSQS = {
    try {
      val isClusterOnEc2Role = hadoopConf.getBoolean(
        "fs.s3.isClusterOnEc2Role", false) || hadoopConf.getBoolean(
        "fs.s3n.isClusterOnEc2Role", false) || sourceOptions.useInstanceProfileCredentials
      if (!isClusterOnEc2Role) {
        val accessKey = hadoopConf.getTrimmed("fs.s3n.awsAccessKeyId")
        val secretAccessKey = new String(hadoopConf.getPassword("fs.s3n.awsSecretAccessKey")).trim
        logInfo("Using credentials from keys provided")
        val basicAwsCredentialsProvider = new BasicAWSCredentialsProvider(
          accessKey, secretAccessKey)
        AmazonSQSClientBuilder
          .standard()
          .withClientConfiguration(new ClientConfiguration().withMaxConnections(maxConnections))
          .withCredentials(basicAwsCredentialsProvider)
          .withRegion(region)
          .build()
      } else {
        logInfo("Using the credentials attached to the instance")
        val instanceProfileCredentialsProvider = new InstanceProfileCredentialsProviderWithRetries()
        AmazonSQSClientBuilder
          .standard()
          .withClientConfiguration(new ClientConfiguration().withMaxConnections(maxConnections))
          .withCredentials(instanceProfileCredentialsProvider)
          .build()
      }
    } catch {
      case e: Exception =>
        throw new SparkException(s"Error occured while creating Amazon SQS Client ${e.getMessage}")
    }
  }

  def addToDeleteMessageQueue(messageReceiptHandles: List[String]): Unit = {
    deleteMessageQueue.addAll(messageReceiptHandles.asJava)
  }

  def deleteMessagesFromQueue(): Unit = {
    try {
      var count = -1
      val messageReceiptHandles = deleteMessageQueue.asScala.toList
      val messageGroups = messageReceiptHandles.sliding(10, 10).toList
      messageGroups.foreach { messageGroup =>
        val requestEntries = messageGroup.foldLeft(List[DeleteMessageBatchRequestEntry]()) {
          (list, messageReceiptHandle) =>
            count = count + 1
            list :+ new DeleteMessageBatchRequestEntry(count.toString, messageReceiptHandle)
        }.asJava
        val batchResult = sqsClient.deleteMessageBatch(sqsUrl, requestEntries)
        if (!batchResult.getFailed.isEmpty) {
          batchResult.getFailed.asScala.foreach { entry =>
            sqsClient.deleteMessage(
              sqsUrl, requestEntries.get(entry.getId.toInt).getReceiptHandle)
          }
        }
      }
    } catch {
      case e: Exception =>
        logWarning(s"Unable to delete message from SQS ${e.getMessage}")
    }
    deleteMessageQueue.clear()
  }

  def assertSqsIsWorking(): Unit = {
    if (exception.isDefined) {
      throw exception.get
    }
  }

}
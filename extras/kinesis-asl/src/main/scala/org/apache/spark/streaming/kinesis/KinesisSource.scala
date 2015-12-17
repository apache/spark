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

import java.util.UUID

import com.amazonaws.services.kinesis.AmazonKinesisClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessorCheckpointer, IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.model.{GetShardIteratorRequest, Record}

import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.Logging

import org.apache.spark.sql.execution.streaming.{Offset, Source}

case class StreamOffset(sequenceNumbers: Map[String, String]) extends Offset {
  override def isEmpty: Boolean = false

  override def >(other: Offset): Boolean = ???

  override def <(other: Offset): Boolean = ???
}

case class KinesisSource(
    endpoint: String,
    regionId: String,
    streamName: String,
    credentials: SerializableAWSCredentials) extends Source {
  private val client = new AmazonKinesisClient(credentials)
  client.setEndpoint(endpoint, "kinesis", regionId)

  override def offset: Offset = {
    val desc = client.describeStream(streamName)
    val endSequenceNumbers = desc.getStreamDescription.getShards.asScala.map { s =>
      val range = s.getSequenceNumberRange
      (s.getShardId, range.getEndingSequenceNumber)
    }.toMap
    StreamOffset(endSequenceNumbers)
  }

  override def getSlice(
      sqlContext: SQLContext,
      start: Offset,
      end: Offset): RDD[InternalRow] = {



    val getShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setRequestCredentials(credentials)
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shardId)
    getShardIteratorRequest.setShardIteratorType(iteratorType.toString)
    getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber)
  }
}

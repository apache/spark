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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.services.kinesis.producer.{KinesisProducer => KPLProducer, KinesisProducerConfiguration, UserRecordResult}
import com.google.common.util.concurrent.{FutureCallback, Futures}

private[kinesis] class KPLBasedKinesisTestUtils(streamShardCount: Int = 2)
    extends KinesisTestUtils(streamShardCount) {
  override protected def getProducer(aggregate: Boolean): KinesisDataGenerator = {
    if (!aggregate) {
      new SimpleDataGenerator(kinesisClient)
    } else {
      new KPLDataGenerator(regionName)
    }
  }
}

/** A wrapper for the KinesisProducer provided in the KPL. */
private[kinesis] class KPLDataGenerator(regionName: String) extends KinesisDataGenerator {

  private lazy val producer: KPLProducer = {
    val conf = new KinesisProducerConfiguration()
      .setRecordMaxBufferedTime(1000)
      .setMaxConnections(1)
      .setRegion(regionName)
      .setMetricsLevel("none")

    new KPLProducer(conf)
  }

  override def sendData(streamName: String, data: Seq[Int]): Map[String, Seq[(Int, String)]] = {
    val shardIdToSeqNumbers = new mutable.HashMap[String, ArrayBuffer[(Int, String)]]()
    data.foreach { num =>
      val str = num.toString
      val data = ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8))
      val future = producer.addUserRecord(streamName, str, data)
      val kinesisCallBack = new FutureCallback[UserRecordResult]() {
        override def onFailure(t: Throwable): Unit = {} // do nothing

        override def onSuccess(result: UserRecordResult): Unit = {
          val shardId = result.getShardId
          val seqNumber = result.getSequenceNumber()
          val sentSeqNumbers = shardIdToSeqNumbers.getOrElseUpdate(shardId,
            new ArrayBuffer[(Int, String)]())
          sentSeqNumbers += ((num, seqNumber))
        }
      }
      Futures.addCallback(future, kinesisCallBack)
    }
    producer.flushSync()
    shardIdToSeqNumbers.toMap
  }
}

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

package org.apache.spark.streaming.dstream

import java.io.{NotSerializableException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.reflect.ClassTag

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming.{StreamingContext, Time}

private[streaming]
class QueueInputDStream[T: ClassTag](
    ssc: StreamingContext,
    val queue: Queue[RDD[T]],
    oneAtATime: Boolean,
    defaultRDD: RDD[T]
  ) extends InputDStream[T](ssc) {

  override def start() { }

  override def stop() { }

  private def readObject(in: ObjectInputStream): Unit = {
    throw new NotSerializableException("queueStream doesn't support checkpointing. " +
      "Please don't use queueStream when checkpointing is enabled.")
  }

  private def writeObject(oos: ObjectOutputStream): Unit = {
    logWarning("queueStream doesn't support checkpointing")
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    val buffer = new ArrayBuffer[RDD[T]]()
    queue.synchronized {
      if (oneAtATime && queue.nonEmpty) {
        buffer += queue.dequeue()
      } else {
        buffer ++= queue
        queue.clear()
      }
    }
    if (buffer.nonEmpty) {
      if (oneAtATime) {
        Some(buffer.head)
      } else {
        Some(new UnionRDD(context.sc, buffer.toSeq))
      }
    } else if (defaultRDD != null) {
      Some(defaultRDD)
    } else {
      Some(ssc.sparkContext.emptyRDD)
    }
  }

}

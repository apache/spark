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

package org.apache.spark.streaming.akka

import scala.reflect.ClassTag

import akka.actor.{ActorSystem, SupervisorStrategy, Props}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaStreamingContext, JavaReceiverInputDStream}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * Factory interface for creating a new ActorSystem in executors.
 */
trait ActorSystemFactory extends Serializable {
  def create(): ActorSystem
}

object AkkaUtils {

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * Find more details at: http://spark.apache.org/docs/latest/streaming-custom-receivers.html
   *
   * @param ssc the StreamingContext instance
   * @param actorSystemCreator a function to create ActorSystem in executors
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   * @param storageLevel RDD storage level (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   * @param supervisorStrategy the supervisor strategy (default:
   *                           ActorSupervisorStrategy.defaultStrategy)
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def createStream[T: ClassTag](
      ssc: StreamingContext,
      actorSystemCreator: () => ActorSystem,
      props: Props,
      name: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
      supervisorStrategy: SupervisorStrategy = ActorSupervisorStrategy.defaultStrategy
      ): ReceiverInputDStream[T] = ssc.withNamedScope("actor stream") {
    val cleanF = ssc.sc.clean(actorSystemCreator)
    ssc.receiverStream(new ActorReceiver[T](cleanF, props, name, storageLevel, supervisorStrategy))
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   *
   * @param jssc the StreamingContext instance
   * @param actorSystemFactory an ActorSystemFactory to create ActorSystem in executors
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   * @param storageLevel Storage level to use for storing the received objects
   * @param supervisorStrategy the supervisor strategy
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      actorSystemFactory: ActorSystemFactory,
      props: Props,
      name: String,
      storageLevel: StorageLevel,
      supervisorStrategy: SupervisorStrategy
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    createStream[T](
      jssc.ssc, () => actorSystemFactory.create(), props, name, storageLevel, supervisorStrategy)
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   *
   * @param jssc the StreamingContext instance
   * @param actorSystemFactory an ActorSystemFactory to create ActorSystem in executors
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   * @param storageLevel Storage level to use for storing the received objects
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      actorSystemFactory: ActorSystemFactory,
      props: Props,
      name: String,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    createStream[T](jssc.ssc, () => actorSystemFactory.create(), props, name, storageLevel)
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   *
   * @param jssc the StreamingContext instance
   * @param actorSystemFactory an ActorSystemFactory to create ActorSystem in executors
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      actorSystemFactory: ActorSystemFactory,
      props: Props,
      name: String
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    createStream[T](jssc.ssc, () => actorSystemFactory.create(), props, name)
  }

}

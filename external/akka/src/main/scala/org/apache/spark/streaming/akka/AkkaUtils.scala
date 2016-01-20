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

import akka.actor.{ActorSystem, Props, SupervisorStrategy}

import org.apache.spark.api.java.function.{Function0 => JFunction0}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object AkkaUtils {

  /**
   * Create an input stream with a user-defined actor. See [[ActorReceiver]] for more details.
   *
   * @param ssc The StreamingContext instance
   * @param propsForActor Props object defining creation of the actor
   * @param actorName Name of the actor
   * @param storageLevel RDD storage level (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   * @param actorSystemCreator A function to create ActorSystem in executors. `ActorSystem` will
   *                           be shut down when the receiver is stopping (default:
   *                           ActorReceiver.defaultActorSystemCreator)
   * @param supervisorStrategy the supervisor strategy (default: ActorReceiver.defaultStrategy)
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e. parametrized type of data received and createStream
   *       should be same.
   */
  def createStream[T: ClassTag](
      ssc: StreamingContext,
      propsForActor: Props,
      actorName: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
      actorSystemCreator: () => ActorSystem = ActorReceiver.defaultActorSystemCreator,
      supervisorStrategy: SupervisorStrategy = ActorReceiver.defaultSupervisorStrategy
    ): ReceiverInputDStream[T] = ssc.withNamedScope("actor stream") {
    val cleanF = ssc.sc.clean(actorSystemCreator)
    ssc.receiverStream(new ActorReceiverSupervisor[T](
      cleanF,
      propsForActor,
      actorName,
      storageLevel,
      supervisorStrategy))
  }

  /**
   * Create an input stream with a user-defined actor. See [[JavaActorReceiver]] for more details.
   *
   * @param jssc The StreamingContext instance
   * @param propsForActor Props object defining creation of the actor
   * @param actorName Name of the actor
   * @param storageLevel Storage level to use for storing the received objects
   * @param actorSystemCreator A function to create ActorSystem in executors. `ActorSystem` will
   *                           be shut down when the receiver is stopping.
   * @param supervisorStrategy the supervisor strategy
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e. parametrized type of data received and createStream
   *       should be same.
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      propsForActor: Props,
      actorName: String,
      storageLevel: StorageLevel,
      actorSystemCreator: JFunction0[ActorSystem],
      supervisorStrategy: SupervisorStrategy
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    createStream[T](
      jssc.ssc,
      propsForActor,
      actorName,
      storageLevel,
      () => actorSystemCreator.call(),
      supervisorStrategy)
  }

  /**
   * Create an input stream with a user-defined actor. See [[JavaActorReceiver]] for more details.
   *
   * @param jssc The StreamingContext instance
   * @param propsForActor Props object defining creation of the actor
   * @param actorName Name of the actor
   * @param storageLevel Storage level to use for storing the received objects
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e. parametrized type of data received and createStream
   *       should be same.
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      propsForActor: Props,
      actorName: String,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    createStream[T](jssc.ssc, propsForActor, actorName, storageLevel)
  }

  /**
   * Create an input stream with a user-defined actor. Storage level of the data will be the default
   * StorageLevel.MEMORY_AND_DISK_SER_2. See [[JavaActorReceiver]] for more details.
   *
   * @param jssc The StreamingContext instance
   * @param propsForActor Props object defining creation of the actor
   * @param actorName Name of the actor
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e. parametrized type of data received and createStream
   *       should be same.
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      propsForActor: Props,
      actorName: String
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    createStream[T](jssc.ssc, propsForActor, actorName)
  }
}

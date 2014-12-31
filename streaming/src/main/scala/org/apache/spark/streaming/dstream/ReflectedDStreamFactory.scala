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

import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
 * Abstract serializable factory for [[org.apache.spark.streaming.dstream.InputDStream]] instances.
 *
 * Intended to be used by the [[org.apache.spark.streaming.StreamingContext]] reflectedStream
 * method.
 *
 * See [[org.apache.spark.streaming.zeromq.ReflectedZeroMQStreamFactory]] for an example concrete
 * implementation.
 *
 * @param streamParams Parameters to be used during stream instantiation.
 * @tparam T Data type handled by the instantiated DStreams.
 */
abstract class ReflectedDStreamFactory[T: ClassTag](streamParams: Seq[String])
extends java.io.Serializable {
  /**
   * Creates a new [[org.apache.spark.streaming.dstream.InputDStream]] instance.
   *
   * Implementations should create a new InputDStream instance of the appropriate
   * type, using the parameters passed in the factory constructor.
   *
   * @param ssc The active StreamingContext.
   * @return new InputDStream
   */
  def instantiateStream(ssc: StreamingContext): InputDStream[T]
}

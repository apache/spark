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

package org.apache.spark.streaming.api.java

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * A Java-friendly interface to [[org.apache.spark.streaming.dstream.ReceiverInputDStream]], the
 * abstract class for defining any input stream that receives data over the network.
 */
class JavaReceiverInputDStream[T](val receiverInputDStream: ReceiverInputDStream[T])
  (implicit override val classTag: ClassTag[T]) extends JavaInputDStream[T](receiverInputDStream) {
}

object JavaReceiverInputDStream {
  /**
   * Convert a scala [[org.apache.spark.streaming.dstream.ReceiverInputDStream]] to a Java-friendly
   * [[org.apache.spark.streaming.api.java.JavaReceiverInputDStream]].
   */
  implicit def fromReceiverInputDStream[T: ClassTag](
      receiverInputDStream: ReceiverInputDStream[T]): JavaReceiverInputDStream[T] = {
    new JavaReceiverInputDStream[T](receiverInputDStream)
  }
}

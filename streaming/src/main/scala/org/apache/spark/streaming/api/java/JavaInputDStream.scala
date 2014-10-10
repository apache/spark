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

import org.apache.spark.streaming.dstream.InputDStream

/**
 * A Java-friendly interface to [[org.apache.spark.streaming.dstream.InputDStream]].
 */
class JavaInputDStream[T](val inputDStream: InputDStream[T])
  (implicit override val classTag: ClassTag[T]) extends JavaDStream[T](inputDStream) {
}

object JavaInputDStream {
  /**
   * Convert a scala [[org.apache.spark.streaming.dstream.InputDStream]] to a Java-friendly
   * [[org.apache.spark.streaming.api.java.JavaInputDStream]].
   */
  implicit def fromInputDStream[T: ClassTag](
      inputDStream: InputDStream[T]): JavaInputDStream[T] = {
    new JavaInputDStream[T](inputDStream)
  }
}

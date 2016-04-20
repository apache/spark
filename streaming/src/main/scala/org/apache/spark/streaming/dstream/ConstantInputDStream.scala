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

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}

/**
 * An input stream that always returns the same RDD on each time step. Useful for testing.
 */
class ConstantInputDStream[T: ClassTag](_ssc: StreamingContext, rdd: RDD[T])
  extends InputDStream[T](_ssc) {

  require(rdd != null,
    "parameter rdd null is illegal, which will lead to NPE in the following transformation")

  override def start() {}

  override def stop() {}

  override def compute(validTime: Time): Option[RDD[T]] = {
    Some(rdd)
  }
}

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

package org.apache.spark.streaming

import org.apache.spark.{Accumulable, AccumulableParam, SparkException}

/**
  * Store information of Accumulable. We can't checkpoint Accumulable dircectly because the
  * "readObject" method of Accumulable to add extra logic.
  */
class AccumulableCheckpoint[R, T] private (
  val name: String,
  val value: R,
  val param: AccumulableParam[R, T]) extends Serializable {

  def this(accu : Accumulable[R, T]) = {
    this(accu.name match {
      case Some(n) => n
      case None => throw new SparkException("Only Accumulable with name could be Checkpointed")
    }, accu.value, accu.param)
  }
}

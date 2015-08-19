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

package org.apache.spark.mllib.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

private[mllib] trait Broadcastable {

  /**
   * Checks whether the model object is already broadcast and returns the reference.
   * If not, then broadcasts the model and returns a reference
   * @param bcReference Option containing the broadcast ref
   * @param rdd RDD that will be used for the broadcast
   * @param modelToBc model object to broadcast
   * @return an Option object containing the broadcast model
   */
  def getBroadcastModel[T,V](bcReference: Option[Broadcast[T]], rdd: RDD[V],
    modelToBc:T) : Option[Broadcast[T]] = {
    bcReference match {
      case None => Some(rdd.context.broadcast(modelToBc))
      case _ => bcReference
    }
  }

}

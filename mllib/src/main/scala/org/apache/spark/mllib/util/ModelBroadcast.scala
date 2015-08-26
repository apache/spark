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

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

private[spark] trait Broadcastable[T] {

  private var bcModel: Option[Broadcast[T]] = None

  /**
   * Checks whether the model object is already broadcast and returns the reference.
   * If not, then broadcasts the model and returns a reference
   * @param sc SparkContext that will be used for the broadcast
   * @param modelToBc Model object to broadcast
   * @return the broadcast model
   */
  def getBroadcastModel(sc: SparkContext, modelToBc: T)
                       (implicit ev: ClassTag[T]) : Broadcast[T] = {
    bcModel match {
      case None => bcModel = Some(sc.broadcast(modelToBc))
      case _ =>
    }
    bcModel.get
  }

}

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

package org.apache.spark.resource

/**
 * An task resource request. This is used in conjuntion with the ResourceProfile to
 * programmatically specify the resources needed for an RDD that will be applied at the
 * stage level.
 *
 * @param resourceName Name of the resource
 * @param amount Amount requesting

 * This api is currently private until the rest of the pieces are in place and then it
 * will become public.
 */
private[spark] class TaskResourceRequest(
    val resourceName: String,
    val amount: Int) extends Serializable {

  override def toString(): String = {
    s"TaskResourceRequest: resourceName = $resourceName, amount = $amount"
  }
}


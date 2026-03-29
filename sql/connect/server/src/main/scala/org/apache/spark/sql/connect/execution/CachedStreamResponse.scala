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

package org.apache.spark.sql.connect.execution

import com.google.protobuf.MessageLite

private[execution] case class CachedStreamResponse[T <: MessageLite](
    // the actual cached response
    response: T,
    // the id of the response, an UUID.
    responseId: String,
    // index of the response in the response stream.
    // responses produced in the stream are numbered consecutively starting from 1.
    streamIndex: Long) {

  def serializedByteSize: Long = response.getSerializedSize.toLong
}

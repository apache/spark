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

package org.apache.spark.sql.connect.common

/**
 * A wrapper class around the StreamingQueryListener and an id associated with it.
 *
 * This class is shared between the client and the server to allow for serialization and
 * deserialization of the JVM object. We'll need to cache a mapping between the id and listener
 * object on server side in order to identify the correct server side listener object.
 *
 * @param id
 *   The id for the StreamingQueryListener.
 * @param listener
 *   The StreamingQueryListener instance.
 */
case class StreamingListenerPacket(id: String, listener: AnyRef) extends Serializable

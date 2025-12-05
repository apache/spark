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

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder

/**
 * A wrapper class around the foreachWriter and it's Input/Output [[AgnosticEncoder]](s).
 *
 * This class is shared between the client and the server to allow for serialization and
 * deserialization of the JVM object.
 *
 * @param foreachWriter
 *   The actual foreachWriter from client
 * @param datasetEncoder
 *   An [[AgnosticEncoder]] for the input row
 */
@SerialVersionUID(3882541391565582579L)
case class ForeachWriterPacket(foreachWriter: AnyRef, datasetEncoder: AgnosticEncoder[_])
    extends Serializable

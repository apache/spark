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

package org.apache.spark.internal.config

import org.apache.spark.network.util.ByteUnit

private[spark] object Kryo {

  val KRYO_REGISTRATION_REQUIRED = ConfigBuilder("spark.kryo.registrationRequired")
    .booleanConf
    .createWithDefault(false)

  val KRYO_USER_REGISTRATORS = ConfigBuilder("spark.kryo.registrator")
    .stringConf
    .createOptional

  val KRYO_CLASSES_TO_REGISTER = ConfigBuilder("spark.kryo.classesToRegister")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val KRYO_USE_UNSAFE = ConfigBuilder("spark.kryo.unsafe")
    .booleanConf
    .createWithDefault(false)

  val KRYO_USE_POOL = ConfigBuilder("spark.kryo.pool")
    .booleanConf
    .createWithDefault(true)

  val KRYO_REFERENCE_TRACKING = ConfigBuilder("spark.kryo.referenceTracking")
    .booleanConf
    .createWithDefault(true)

  val KRYO_SERIALIZER_BUFFER_SIZE = ConfigBuilder("spark.kryoserializer.buffer")
    .bytesConf(ByteUnit.KiB)
    .createWithDefaultString("64k")

  val KRYO_SERIALIZER_MAX_BUFFER_SIZE = ConfigBuilder("spark.kryoserializer.buffer.max")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("64m")

}

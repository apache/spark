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

// Deterministic fixtures for UdfSerializationSuite. Each pair has same-length class names, so a
// serialized stream of the *V1 class can be turned into a *V2 stream by an in-place class-name
// patch, letting a single build simulate a producer (*V1) and a consumer (*V2) whose
// serialVersionUIDs differ.

package org.apache.spark.sql.types {
  // Default field serialization, computed SUID, identical layout.
  case class SuidCompatV1(a: Int, b: String)
  case class SuidCompatV2(a: Int, b: String)

  // Different persistent field layout.
  case class SuidLayoutV1(a: Int, b: String)
  case class SuidLayoutV2(a: Int)

  // The consumer declares a serialVersionUID.
  @SerialVersionUID(4242L) case class SuidExplicitV1(a: Int, b: String)
  @SerialVersionUID(4243L) case class SuidExplicitV2(a: Int, b: String)

  // The consumer has a custom readObject.
  case class SuidCustomV1(a: Int, b: String) {
    private def readObject(in: java.io.ObjectInputStream): Unit = in.defaultReadObject()
  }
  case class SuidCustomV2(a: Int, b: String) {
    private def readObject(in: java.io.ObjectInputStream): Unit = in.defaultReadObject()
  }

  // Only the producer writes custom class data; the consumer uses default serialization.
  case class SuidProducerCustomV1(a: Int, b: String) {
    private def writeObject(out: java.io.ObjectOutputStream): Unit = {
      out.defaultWriteObject()
      out.writeInt(42)
    }
  }
  case class SuidProducerCustomV2(a: Int, b: String)

  // Only the producer declares a serialVersionUID; the consumer's is computed.
  @SerialVersionUID(4244L) case class SuidProducerExplicitV1(a: Int, b: String)
  case class SuidProducerExplicitV2(a: Int, b: String)
}

package org.apache.spark.sql.connect.common {
  // Outside the tolerant `sql.types` package.
  case class SuidUntolerantV1(a: Int, b: String)
  case class SuidUntolerantV2(a: Int, b: String)
}

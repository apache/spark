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
// serialized stream of the *V1 class can be turned into a *V2 stream by an in-place, same-length
// class-name patch, letting a single build simulate a cross-version producer/consumer pair whose
// serialVersionUIDs differ (the two auto-computed SUIDs differ because the class name differs).

package org.apache.spark.sql.types {
  // Plain field-serialized, auto-computed SUID, identical layout: tolerated (rebind succeeds).
  case class SuidCompatV1(a: Int, b: String)
  case class SuidCompatV2(a: Int, b: String)

  // Different serialized field layout: not tolerated (must fail fast).
  case class SuidLayoutV1(a: Int, b: String)
  case class SuidLayoutV2(a: Int)

  // Explicit serialVersionUID: an explicit-SUID change is a deliberate break, not tolerated.
  @SerialVersionUID(4242L) case class SuidExplicitV1(a: Int, b: String)
  @SerialVersionUID(4243L) case class SuidExplicitV2(a: Int, b: String)

  // Custom readObject protocol: descriptor substitution is unsafe, not tolerated.
  case class SuidCustomV1(a: Int, b: String) {
    private def readObject(in: java.io.ObjectInputStream): Unit = in.defaultReadObject()
  }
  case class SuidCustomV2(a: Int, b: String) {
    private def readObject(in: java.io.ObjectInputStream): Unit = in.defaultReadObject()
  }
}

package org.apache.spark.sql.connect.common {
  // Outside the tolerant `sql.types` package: SUID drift must NOT be tolerated.
  case class SuidUntolerantV1(a: Int, b: String)
  case class SuidUntolerantV2(a: Int, b: String)
}

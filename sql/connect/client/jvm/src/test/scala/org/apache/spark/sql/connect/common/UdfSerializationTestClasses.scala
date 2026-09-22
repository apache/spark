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

// Deterministic fixtures for UdfSerializationSuite. Each pair has same-length class names so a
// serialized stream of the *V1 class can be turned into a *V2 stream by an in-place, same-length
// class-name patch, letting a single build simulate a cross-version producer/consumer pair.
// The `sql.types`-package pairs exercise the SUID-tolerant path; the other pair verifies scoping.

package org.apache.spark.sql.types {
  // Same serialized field layout, different serialVersionUID: tolerated (rebind succeeds).
  @SerialVersionUID(1001L) case class SuidCompatV1(a: Int, b: String)
  @SerialVersionUID(1002L) case class SuidCompatV2(a: Int, b: String)

  // Different serialized field layout: not tolerated (must fail fast).
  @SerialVersionUID(2001L) case class SuidLayoutV1(a: Int, b: String)
  @SerialVersionUID(2002L) case class SuidLayoutV2(a: Int)
}

package org.apache.spark.sql.connect.common {
  // Outside the tolerant `sql.types` package: SUID drift must NOT be tolerated.
  @SerialVersionUID(3001L) case class SuidUntolerantV1(a: Int, b: String)
  @SerialVersionUID(3002L) case class SuidUntolerantV2(a: Int, b: String)
}

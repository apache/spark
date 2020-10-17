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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite

class CaseInsensitiveMapSuite extends SparkFunSuite {
  test("SPARK-32377: CaseInsensitiveMap should be deterministic for addition") {
     var m = CaseInsensitiveMap(Map.empty[String, String])
     Seq(("paTh", "1"), ("PATH", "2"), ("Path", "3"), ("patH", "4"), ("path", "5")).foreach { kv =>
       m = (m + kv).asInstanceOf[CaseInsensitiveMap[String]]
       assert(m.get("path").contains(kv._2))
     }
  }
}

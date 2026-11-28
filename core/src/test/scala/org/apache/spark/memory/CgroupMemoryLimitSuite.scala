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

package org.apache.spark.memory

import org.apache.spark.SparkFunSuite

class CgroupMemoryLimitSuite extends SparkFunSuite {
  test("parse bounded cgroup limits") {
    assert(CgroupMemoryLimit.parseLimit("1572864000").contains(1572864000L))
    assert(CgroupMemoryLimit.parseLimit("max").isEmpty)
    assert(CgroupMemoryLimit.parseLimit(Long.MaxValue.toString).isEmpty)
    assert(CgroupMemoryLimit.parseLimit("invalid").isEmpty)
  }

  test("parse cgroup v1 and v2 process paths") {
    assert(CgroupMemoryLimit.v2Path(Seq("0::/kubepods/pod1")).contains("/kubepods/pod1"))
    assert(CgroupMemoryLimit.v1MemoryPath(Seq(
      "8:cpu,cpuacct:/docker/abc",
      "2:memory:/docker/abc")).contains("/docker/abc"))
  }

  test("detect configured memory exceeding the cgroup limit") {
    val gib = 1024L * 1024L * 1024L
    assert(CgroupMemoryLimit.exceedsLimit(gib, 2L * gib, 1500L * 1024L * 1024L))
    assert(!CgroupMemoryLimit.exceedsLimit(gib, 256L * 1024L * 1024L, 2L * gib))
  }
}

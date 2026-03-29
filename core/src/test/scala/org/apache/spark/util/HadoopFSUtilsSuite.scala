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

package org.apache.spark.util

import org.apache.spark.SparkFunSuite

class HadoopFSUtilsSuite extends SparkFunSuite {
  test("HadoopFSUtils - file filtering") {
    assert(!HadoopFSUtils.shouldFilterOutPathName("abcd"))
    assert(HadoopFSUtils.shouldFilterOutPathName(".ab"))
    assert(HadoopFSUtils.shouldFilterOutPathName("_cd"))
    assert(!HadoopFSUtils.shouldFilterOutPathName("_metadata"))
    assert(!HadoopFSUtils.shouldFilterOutPathName("_common_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPathName("_ab_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPathName("_cd_common_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPathName("a._COPYING_"))
  }

  test("SPARK-45452: HadoopFSUtils - path filtering") {
    // Case 1: Regular and metadata paths
    assert(!HadoopFSUtils.shouldFilterOutPath("/abcd"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/abcd/efg"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/year=2023/month=10/day=8/hour=13"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/part=__HIVE_DEFAULT_PARTITION__"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_cd=123"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_cd=123/1"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_metadata"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_metadata/1"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_common_metadata"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_common_metadata/1"))
    // Case 2: Hidden paths and the paths ending `._COPYING_`
    assert(HadoopFSUtils.shouldFilterOutPath("/.ab"))
    assert(HadoopFSUtils.shouldFilterOutPath("/.ab/cde"))
    assert(HadoopFSUtils.shouldFilterOutPath("/.ab/_metadata/1"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde/fg"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde/_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde/_common_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde/year=2023/month=10/day=8/hour=13"))
    assert(HadoopFSUtils.shouldFilterOutPath("/x/.hidden/part=__HIVE_DEFAULT_PARTITION__"))
    assert(HadoopFSUtils.shouldFilterOutPath("/a._COPYING_"))
    // Case 3: Underscored paths (except metadata paths of Case 1)
    assert(HadoopFSUtils.shouldFilterOutPath("/_cd"))
    assert(HadoopFSUtils.shouldFilterOutPath("/_cd/1"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/_cd/1"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/_cd/part=1"))
    assert(HadoopFSUtils.shouldFilterOutPath("/_ab_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPath("/_cd_common_metadata"))
  }
}

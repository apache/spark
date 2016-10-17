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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkFunSuite

class PartitioningAwareFileCatalogSuite extends SparkFunSuite {

  test("file filtering") {
    assert(!PartitioningAwareFileCatalog.shouldFilterOut("abcd"))
    assert(PartitioningAwareFileCatalog.shouldFilterOut(".ab"))
    assert(PartitioningAwareFileCatalog.shouldFilterOut("_cd"))

    assert(!PartitioningAwareFileCatalog.shouldFilterOut("_metadata"))
    assert(!PartitioningAwareFileCatalog.shouldFilterOut("_common_metadata"))
    assert(PartitioningAwareFileCatalog.shouldFilterOut("_ab_metadata"))
    assert(PartitioningAwareFileCatalog.shouldFilterOut("_cd_common_metadata"))
  }
}

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

package org.apache.spark.sql.kafka010

import org.apache.spark.sql.streaming.OffsetSuite

class KafkaSourceOffsetSuite extends OffsetSuite {

  compare(
    one = KafkaSourceOffset(("t", 0, 1L)),
    two = KafkaSourceOffset(("t", 0, 2L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L), ("t", 1, 0L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("t", 1, 1L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L), ("T", 0, 0L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("T", 0, 1L)))

  compare(
    one = KafkaSourceOffset(("t", 0, 1L)),
    two = KafkaSourceOffset(("t", 0, 2L), ("t", 1, 1L)))
}

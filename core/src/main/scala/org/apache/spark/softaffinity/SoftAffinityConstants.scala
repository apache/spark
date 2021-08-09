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

package org.apache.spark.softaffinity

object SoftAffinityConstants {

  val PARAMS_KEY_ENABLED_SOFT_AFFINITY = "spark.enabled.soft-affinity"

  val PARAMS_KEY_ENABLED_SOFT_AFFINITY_DEFAULT_VALUE = false

  val PARAMS_KEY_SOFT_AFFINITY_REPLICATION_NUM = "spark.soft-affinity.replication-num"

  val PARAMS_KEY_SOFT_AFFINITY_REPLICATION_NUM_DEFAULT_VALUE = 2

  val PARAMS_KEY_SOFT_AFFINITY_MIN_TARGET_HOSTS = "spark.soft-affinity.min.target-hosts"

  val PARAMS_KEY_SOFT_AFFINITY_MIN_TARGET_HOSTS_DEFAULT_VALUE = 1
}

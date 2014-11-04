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

package org.apache.spark.ui.jobs

/**
 * Names of the CSS classes corresponding to each type of task detail. Used to allow users
 * to optionally show/hide columns.
 */
private object TaskDetailsClassNames {
  val SCHEDULER_DELAY = "scheduler_delay"
  val GC_TIME = "gc_time"
  val RESULT_SERIALIZATION_TIME = "serialization_time"
  val GETTING_RESULT_TIME = "getting_result_time"
}

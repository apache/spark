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
package org.apache.spark.udf.worker.core

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Identifies a security boundary for worker connection pooling.
 *
 * Workers are only reused within the same security scope. Dispatcher
 * implementations compare scopes using `equals` to decide whether an
 * existing worker can be shared. Subclasses '''must''' override
 * `equals` and `hashCode` so that structurally equivalent scopes
 * match; otherwise, worker reuse will silently fail.
 */
@Experimental
abstract class WorkerSecurityScope {
  override def equals(obj: Any): Boolean
  override def hashCode(): Int
}

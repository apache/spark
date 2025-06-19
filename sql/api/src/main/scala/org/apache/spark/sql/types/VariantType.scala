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

package org.apache.spark.sql.types

import org.apache.spark.annotation.Unstable

/**
 * The data type representing semi-structured values with arbitrary hierarchical data structures.
 * It is intended to store parsed JSON values and most other data types in the system (e.g., it
 * cannot store a map with a non-string key type).
 *
 * @since 4.0.0
 */
@Unstable
class VariantType private () extends AtomicType {
  // The default size is used in query planning to drive optimization decisions. 2048 is arbitrarily
  // picked and we currently don't have any data to support it. This may need revisiting later.
  override def defaultSize: Int = 2048

  /** This is a no-op because values with VARIANT type are always nullable. */
  private[spark] override def asNullable: VariantType = this
}

/**
 * @since 4.0.0
 */
@Unstable
case object VariantType extends VariantType

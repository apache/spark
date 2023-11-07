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

import org.apache.spark.annotation.Stable

/**
 * The data type representing semi-structured values with arbitrary hierarchical data structures. At
 * this moment, it is intended to store parsed JSON values and almost any other data types in the
 * system (e.g., we don't plan to let it store a map with a non-string key type). In the future, we
 * may also extend it to store other semi-structured data representation like XML.
 */
@Stable
class VariantType private () extends AtomicType {
  // The default size is used in query planning to drive optimization decisions. 2048 is arbitrarily
  // picked and we currently don't have any data to support it. This may need revisiting later.
  override def defaultSize: Int = 2048

  /** This is a no-op because values with VARIANT type are always nullable. */
  private[spark] override def asNullable: VariantType = this
}

@Stable
case object VariantType extends VariantType

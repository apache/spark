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
package org.apache.spark.sql.classic

import scala.language.implicitConversions

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._

/**
 * Conversions from sql interfaces to the Classic specific implementation.
 *
 * This class is mainly used by the implementation, but is also meant to be used by extension
 * developers.
 *
 * We provide both a trait and an object. The trait is useful in situations where an extension
 * developer needs to use these conversions in a project covering multiple Spark versions. They can
 * create a shim for these conversions, the Spark 4+ version of the shim implements this trait, and
 * shims for older versions do not.
 */
@DeveloperApi
trait ClassicConversions {
  implicit def castToImpl(session: api.SparkSession): SparkSession =
    session.asInstanceOf[SparkSession]

  implicit def castToImpl[T](ds: api.Dataset[T]): Dataset[T] =
    ds.asInstanceOf[Dataset[T]]

  implicit def castToImpl(rgds: api.RelationalGroupedDataset): RelationalGroupedDataset =
    rgds.asInstanceOf[RelationalGroupedDataset]

  implicit def castToImpl[K, V](kvds: api.KeyValueGroupedDataset[K, V])
  : KeyValueGroupedDataset[K, V] = kvds.asInstanceOf[KeyValueGroupedDataset[K, V]]
}

object ClassicConversions extends ClassicConversions

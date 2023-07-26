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
package org.apache.spark.sql.connect.client.arrow

import scala.collection.generic.{GenericCompanion, GenMapFactory}
import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.sql.connect.client.arrow.ArrowDeserializers.resolveCompanion

/**
 * A couple of scala version specific collection utility functions.
 */
private[arrow] object ScalaCollectionUtils {
  def getIterableCompanion(tag: ClassTag[_]): GenericCompanion[Iterable] = {
    ArrowDeserializers.resolveCompanion[GenericCompanion[Iterable]](tag)
  }
  def getMapCompanion(tag: ClassTag[_]): GenMapFactory[Map] = {
    resolveCompanion[GenMapFactory[Map]](tag)
  }
  def wrap[T](array: AnyRef): mutable.WrappedArray[T] = {
    mutable.WrappedArray.make(array)
  }
}

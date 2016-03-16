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

package org.apache.spark.serializer

import scala.reflect.ClassTag

import org.apache.spark.SparkConf

/**
 * Component that selects which [[Serializer]] to use for shuffles.
 */
private[spark] class SerializerManager(defaultSerializer: Serializer, conf: SparkConf) {

  private[this] val kryoSerializer = new KryoSerializer(conf)

  private[this] val primitiveAndPrimitiveArrayClassTags: Set[ClassTag[_]] = {
    val primitiveClassTags = Set[ClassTag[_]](
      ClassTag.Boolean,
      ClassTag.Byte,
      ClassTag.Char,
      ClassTag.Double,
      ClassTag.Float,
      ClassTag.Int,
      ClassTag.Long,
      ClassTag.Null,
      ClassTag.Short
    )
    val arrayClassTags = primitiveClassTags.map(_.wrap)
    primitiveClassTags ++ arrayClassTags
  }

  private[this] val stringClassTag: ClassTag[String] = implicitly[ClassTag[String]]

  private def canUseKryo(ct: ClassTag[_]): Boolean = {
    primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
  }

  def getSerializer(ct: ClassTag[_]): Serializer = {
    if (canUseKryo(ct)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  /**
   * Pick the best serializer for shuffling an RDD of key-value pairs.
   */
  def getSerializer(keyClassTag: ClassTag[_], valueClassTag: ClassTag[_]): Serializer = {
    if (canUseKryo(keyClassTag) && canUseKryo(valueClassTag)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }
}

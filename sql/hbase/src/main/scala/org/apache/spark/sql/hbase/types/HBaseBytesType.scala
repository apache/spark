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

package org.apache.spark.sql.hbase.types

import org.apache.spark.sql.types._

import scala.reflect.runtime.universe.typeTag

/**
 * Almost identical to BinaryType except for a different ordering to be consistent
 * with that of HBase's internal ordering
 * This is a data type for Low-Level HBase entities.
 * It should not be used in High-Level processing
 */
private[hbase] case object HBaseBytesType extends NativeType with PrimitiveType {
  override def defaultSize: Int = 4096
  private[sql] type JvmType = Array[Byte]
  // TODO: can not use ScalaReflectionLock now for its accessibility
  // @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  @transient private[sql] lazy val tag = synchronized(typeTag[JvmType])
  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      for (i <- 0 until x.length; if i < y.length) {
        val a: Int = x(i) & 0xff
        val b: Int = y(i) & 0xff
        val res = a - b
        if (res != 0) return res
      }
      x.length - y.length
    }
  }
}

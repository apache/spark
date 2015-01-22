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
package org.apache.spark.sql.hbase.util

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import scala.collection.mutable.ArrayBuffer

object InsertWrappers {

  class ImmutableBytesWritableWrapper(rowKey: Array[Byte])
    extends Serializable {

    def compareTo(that: ImmutableBytesWritableWrapper): Int = {
      this.toImmutableBytesWritable compareTo that.toImmutableBytesWritable
    }

    def toImmutableBytesWritable = new ImmutableBytesWritable(rowKey)
  }

  implicit object ImmutableBytesWritableWrapperOrdering
    extends Ordering[ImmutableBytesWritableWrapper] {
    def compare(a: ImmutableBytesWritableWrapper, b: ImmutableBytesWritableWrapper) = a.compareTo(b)
  }

  class PutWrapper(rowKey: Array[Byte]) extends Serializable {
    val fqv = new ArrayBuffer[(Array[Byte], Array[Byte], Array[Byte])]

    def add(family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]) =
      fqv += ((family, qualifier, value))

    def toPut = {
      val put = new Put(rowKey)
      fqv.foreach { fqv =>
        put.add(fqv._1, fqv._2, fqv._3)
      }
      put
    }
  }
}

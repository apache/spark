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

package org.apache.spark.api.java

import com.google.common.base.Optional

import java.{util => ju}
import scala.collection.mutable

private[spark] object JavaUtils {
  def optionToOptional[T](option: Option[T]): Optional[T] =
    option match {
      case Some(value) => Optional.of(value)
      case None => Optional.absent()
    }

  // Workaround for SPARK-3926 / SI-8911
  def mapAsSerializableJavaMap[A, B](underlying: collection.Map[A, B]) =
    new SerializableMapWrapper(underlying)

  // Implementation is copied from scala.collection.convert.Wrappers.MapWrapper,
  // but implements java.io.Serializable. It can't just be subclassed to make it
  // Serializable since the MapWrapper class has no no-arg constructor. This class
  // doesn't need a no-arg constructor though.
  class SerializableMapWrapper[A, B](underlying: collection.Map[A, B])
    extends ju.AbstractMap[A, B] with java.io.Serializable { self =>

    override def size = underlying.size

    override def get(key: AnyRef): B = try {
      underlying get key.asInstanceOf[A] match {
        case None => null.asInstanceOf[B]
        case Some(v) => v
      }
    } catch {
      case ex: ClassCastException => null.asInstanceOf[B]
    }

    override def entrySet: ju.Set[ju.Map.Entry[A, B]] = new ju.AbstractSet[ju.Map.Entry[A, B]] {
      def size = self.size

      def iterator = new ju.Iterator[ju.Map.Entry[A, B]] {
        val ui = underlying.iterator
        var prev : Option[A] = None

        def hasNext = ui.hasNext

        def next() = {
          val (k, v) = ui.next
          prev = Some(k)
          new ju.Map.Entry[A, B] {
            import scala.util.hashing.byteswap32
            def getKey = k
            def getValue = v
            def setValue(v1 : B) = self.put(k, v1)
            override def hashCode = byteswap32(k.hashCode) + (byteswap32(v.hashCode) << 16)
            override def equals(other: Any) = other match {
              case e: ju.Map.Entry[_, _] => k == e.getKey && v == e.getValue
              case _ => false
            }
          }
        }

        def remove() {
          prev match {
            case Some(k) =>
              underlying match {
                case mm: mutable.Map[A, _] =>
                  mm remove k
                  prev = None
                case _ =>
                  throw new UnsupportedOperationException("remove")
              }
            case _ =>
              throw new IllegalStateException("next must be called at least once before remove")
          }
        }
      }
    }
  }
}

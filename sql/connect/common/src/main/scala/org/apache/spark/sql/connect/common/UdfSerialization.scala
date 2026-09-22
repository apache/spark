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
package org.apache.spark.sql.connect.common

import java.io.{ByteArrayInputStream, InputStream, ObjectInputStream, ObjectStreamClass}

/**
 * Java deserialization for Scala UDF payloads that tolerates the `serialVersionUID` drift of
 * `org.apache.spark.sql.types` classes between Spark versions.
 *
 * A Scala UDF payload embeds `org.apache.spark.sql.types` classes (the UDF's input/output schema).
 * Those classes carry no explicit `@SerialVersionUID`, so the JVM auto-computes it from the whole
 * class shape, which folds in compiler-synthesized members that are irrelevant to serialization --
 * most notably the `$anonfun$` public static methods Scala emits for lambdas. A source change that
 * only reshapes a lambda (e.g. rewriting a helper to use `existsRecursively { ... }`) changes the
 * auto-computed `serialVersionUID` without changing any serialized field, which makes a plain
 * [[ObjectInputStream]] reject a payload produced by a different Spark version with an
 * `InvalidClassException`, even though the payload is field-compatible.
 *
 * The property that actually governs compatibility is the serialized field layout. When a
 * `sql.types` class arrives with a mismatched SUID but an identical serialized field layout, this
 * reader rebinds the stream descriptor to the local class; any field-layout difference is left
 * untouched so the standard SUID check still fails fast rather than misreading the stream.
 *
 * Only `org.apache.spark.sql.types` descriptors are treated tolerantly; every other class keeps
 * the standard `serialVersionUID` compatibility check.
 */
private[spark] object UdfSerialization {

  private val suidTolerantPackagePrefix = "org.apache.spark.sql.types."

  /** Deserialize `bytes` resolving classes with `loader`, tolerating `sql.types` SUID drift. */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val ois = new SuidTolerantObjectInputStream(new ByteArrayInputStream(bytes), loader)
    try ois.readObject().asInstanceOf[T]
    finally ois.close()
  }

  /** Deserialize from `in` with default class resolution, tolerating `sql.types` SUID drift. */
  def deserialize[T](in: InputStream): T = {
    new SuidTolerantObjectInputStream(in, null).readObject().asInstanceOf[T]
  }

  /**
   * The complete serialized field layout of a descriptor: the set of persistent field name + JVM
   * type signature. This is what governs whether [[ObjectInputStream]] can consume the producer's
   * class-data block through the local descriptor. Every persistent slot is included, in
   * particular the Scala lazy-val init `bitmap$*` slots: rebinding to a local descriptor whose
   * slot shape differs would misalign the stream, so a bitmap difference must also block rebinding.
   */
  private[connect] def fieldSignature(desc: ObjectStreamClass): Set[String] = {
    // getTypeString is null for primitives, where the single-char type code is the signature.
    desc.getFields
      .map(f => s"${f.getName}:${Option(f.getTypeString).getOrElse(f.getTypeCode.toString)}")
      .toSet
  }

  private class SuidTolerantObjectInputStream(in: InputStream, loader: ClassLoader)
    extends ObjectInputStream(in) {

    override def resolveClass(desc: ObjectStreamClass): Class[_] = {
      if (loader != null) {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } else {
        super.resolveClass(desc)
      }
    }

    override def readClassDescriptor(): ObjectStreamClass = {
      val streamDesc = super.readClassDescriptor()
      if (!streamDesc.getName.startsWith(suidTolerantPackagePrefix)) {
        return streamDesc
      }
      val localClass =
        try {
          resolveClass(streamDesc)
        } catch {
          case _: ClassNotFoundException => return streamDesc
        }
      val localDesc = ObjectStreamClass.lookup(localClass)
      if (localDesc == null ||
        localDesc.getSerialVersionUID == streamDesc.getSerialVersionUID ||
        fieldSignature(streamDesc) != fieldSignature(localDesc)) {
        // Same class (no drift), unknown class, or a genuine field-layout change: keep the stream
        // descriptor so the standard SUID check applies (and fails fast on a real change).
        streamDesc
      } else {
        localDesc
      }
    }
  }
}

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

import java.io.{ByteArrayInputStream, Externalizable, InputStream, ObjectInputStream}
import java.io.{ObjectOutputStream, ObjectStreamClass}
import java.lang.reflect.Modifier

/**
 * Java deserialization for Scala UDF payloads that tolerates audited `serialVersionUID` changes
 * of `org.apache.spark.sql.types` classes between Spark versions.
 *
 * A Scala UDF payload embeds `org.apache.spark.sql.types` classes (the UDF's input/output
 * schema). Many of them, e.g. the `DataType` singletons, `StringType` and `Decimal`, do not
 * declare a `serialVersionUID`, so the JVM computes it from the whole class shape, including
 * members that are irrelevant to serialization such as the `$anonfun$` methods Scala emits for
 * lambdas. Such a class can change its `serialVersionUID` between releases without changing its
 * serialized form, and a plain [[ObjectInputStream]] then rejects a payload produced by another
 * Spark version with an `InvalidClassException`.
 *
 * A stream descriptor flags custom `writeObject` data, but its numeric SUID does not reveal
 * whether the producer declared or computed that SUID. Tolerance is therefore limited to the
 * exact transitions in [[auditedTransitions]]. A class descriptor is rebound to the local class
 * only when its (class, stream SUID, local SUID) triple is audited, the local class uses default
 * field serialization with a computed SUID, and the complete persistent field layout of the
 * stream and local descriptors is identical. Anything else keeps the stream descriptor, so the
 * standard `serialVersionUID` check applies.
 */
private[spark] object UdfSerialization {

  /** An audited `serialVersionUID` change of one class from a producer build to this build. */
  private[connect] case class SuidTransition(className: String, streamSuid: Long, localSuid: Long)

  /**
   * `serialVersionUID` changes from released Spark versions to this build, audited against the
   * published `spark-sql-api_2.13` jars of 4.0.0 to 4.0.4, 4.1.0 to 4.1.3 and 4.2.0. For every
   * entry, the released and the local class both use default field serialization (no
   * `readObject`/`writeObject`, not `Externalizable`), neither declares a `serialVersionUID`, and
   * their `ObjectStreamClass.getFields` are identical. `localSuid` must match this build, which
   * `UdfSerializationSuite` enforces: any change to these classes' computed `serialVersionUID`
   * requires re-auditing the affected entries.
   */
  private[connect] val auditedTransitions: Set[SuidTransition] = Set(
    SuidTransition(
      "org.apache.spark.sql.types.BinaryType$",
      4981368476672807243L,
      8371054193783994026L),
    SuidTransition(
      "org.apache.spark.sql.types.BooleanType$",
      5066630839557347008L,
      7944921333908111375L),
    SuidTransition(
      "org.apache.spark.sql.types.ByteType$",
      -2622274212739253403L,
      -758215751304624994L),
    SuidTransition(
      "org.apache.spark.sql.types.CalendarIntervalType$",
      -2877907547147508957L,
      3785141433624333846L),
    SuidTransition(
      "org.apache.spark.sql.types.CharType$",
      2241144916566246639L,
      7142653180585234499L),
    SuidTransition(
      "org.apache.spark.sql.types.DateType$",
      -5061141662885574084L,
      4147040835737091283L),
    SuidTransition(
      "org.apache.spark.sql.types.Decimal",
      1715621871942419369L,
      6398599065815978361L),
    SuidTransition(
      "org.apache.spark.sql.types.Decimal$",
      4103410110050351305L,
      622493820172246208L),
    SuidTransition(
      "org.apache.spark.sql.types.DoubleType$",
      8550059415794444422L,
      8798493769972832591L),
    SuidTransition(
      "org.apache.spark.sql.types.FloatType$",
      -6368152288801752152L,
      6520180645217752115L),
    SuidTransition(
      "org.apache.spark.sql.types.IntegerType$",
      5392303096310563711L,
      7538148419353647761L),
    SuidTransition(
      "org.apache.spark.sql.types.LongType$",
      5556619015085473997L,
      -1069125617143205720L),
    SuidTransition(
      "org.apache.spark.sql.types.NullType$",
      4636924136831884463L,
      5274842102097366055L),
    SuidTransition(
      "org.apache.spark.sql.types.ShortType$",
      4196314124164507241L,
      -1640260345880352154L),
    SuidTransition(
      "org.apache.spark.sql.types.StringHelper$",
      -2950363081197529498L,
      5684776218413467458L),
    SuidTransition(
      "org.apache.spark.sql.types.StringType",
      2313622123462496566L,
      -7943010735412336143L),
    SuidTransition(
      "org.apache.spark.sql.types.StringType$",
      -6884398962421839287L,
      -3522724403804736810L),
    SuidTransition(
      "org.apache.spark.sql.types.StructType$",
      4125835014175492466L,
      -2680503192028675703L),
    SuidTransition(
      "org.apache.spark.sql.types.TimestampNTZType$",
      -1053634932597855477L,
      6615591828519817938L),
    SuidTransition(
      "org.apache.spark.sql.types.TimestampType$",
      -6671223943884622784L,
      3549865156948637452L),
    SuidTransition(
      "org.apache.spark.sql.types.UDTRegistration$",
      -4424248461314799595L,
      -3417430582631602433L),
    SuidTransition(
      "org.apache.spark.sql.types.UserDefinedType",
      941169123764985454L,
      7081765465869405043L),
    SuidTransition(
      "org.apache.spark.sql.types.VarcharType$",
      1412037499997274523L,
      1465122347601003917L),
    SuidTransition(
      "org.apache.spark.sql.types.VariantType$",
      3207924491175853678L,
      -8985635318007546424L))

  private val suidTolerantPackagePrefix = "org.apache.spark.sql.types."

  /** Deserialize `bytes` resolving classes with `loader`, tolerating audited SUID changes. */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T =
    deserialize(bytes, loader, auditedTransitions)

  /** Deserialize from `in` with default class resolution, tolerating audited SUID changes. */
  def deserialize[T](in: InputStream): T = deserialize(in, auditedTransitions)

  private[connect] def deserialize[T](
      bytes: Array[Byte],
      loader: ClassLoader,
      transitions: Set[SuidTransition]): T = {
    val ois =
      new SuidTolerantObjectInputStream(new ByteArrayInputStream(bytes), loader, transitions)
    try ois.readObject().asInstanceOf[T]
    finally ois.close()
  }

  private[connect] def deserialize[T](in: InputStream, transitions: Set[SuidTransition]): T = {
    new SuidTolerantObjectInputStream(in, null, transitions).readObject().asInstanceOf[T]
  }

  /**
   * The complete serialized field layout of a descriptor: the set of persistent field name + JVM
   * type signature. Every persistent slot is included, in particular the Scala lazy-val init
   * `bitmap$*` slots: rebinding to a local descriptor whose slot shape differs would misalign the
   * stream, so any slot difference must block rebinding.
   */
  private[connect] def fieldSignature(desc: ObjectStreamClass): Set[String] = {
    // getTypeString is null for primitives, where the single-char type code is the signature.
    desc.getFields
      .map(f => s"${f.getName}:${Option(f.getTypeString).getOrElse(f.getTypeCode.toString)}")
      .toSet
  }

  /**
   * Whether the local class can consume a rebound descriptor: it must use default field-based
   * serialization (no custom `readObject`/`writeObject` and not `Externalizable`, whose protocol
   * `ObjectInputStream` takes from the descriptor) and must not declare a `serialVersionUID`.
   */
  private[connect] def isRebindSafe(clazz: Class[_]): Boolean = {
    !classOf[Externalizable].isAssignableFrom(clazz) &&
    !hasExplicitSerialVersionUID(clazz) &&
    !hasCustomSerialization(clazz)
  }

  private def hasExplicitSerialVersionUID(clazz: Class[_]): Boolean = {
    try {
      val field = clazz.getDeclaredField("serialVersionUID")
      Modifier.isStatic(field.getModifiers) && field.getType == java.lang.Long.TYPE
    } catch {
      case _: NoSuchFieldException => false
    }
  }

  private def hasCustomSerialization(clazz: Class[_]): Boolean = {
    def declares(name: String, paramType: Class[_]): Boolean = {
      try {
        clazz.getDeclaredMethod(name, paramType)
        true
      } catch {
        case _: NoSuchMethodException => false
      }
    }
    declares("readObject", classOf[ObjectInputStream]) ||
    declares("writeObject", classOf[ObjectOutputStream])
  }

  private class SuidTolerantObjectInputStream(
      in: InputStream,
      loader: ClassLoader,
      transitions: Set[SuidTransition])
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
      val name = streamDesc.getName
      if (!name.startsWith(suidTolerantPackagePrefix)) {
        return streamDesc
      }
      val localClass =
        try {
          resolveClass(streamDesc)
        } catch {
          case _: ClassNotFoundException => return streamDesc
        }
      val localDesc = ObjectStreamClass.lookup(localClass)
      if (localDesc != null &&
        transitions.contains(
          SuidTransition(name, streamDesc.getSerialVersionUID, localDesc.getSerialVersionUID)) &&
        isRebindSafe(localClass) &&
        fieldSignature(streamDesc) == fieldSignature(localDesc)) {
        localDesc
      } else {
        // Not an audited transition (including an unchanged SUID), or a local class or layout
        // that cannot safely consume the stream: keep the stream descriptor so the standard
        // serialVersionUID check applies.
        streamDesc
      }
    }
  }
}

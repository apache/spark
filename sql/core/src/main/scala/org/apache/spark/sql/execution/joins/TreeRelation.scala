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

package org.apache.spark.sql.execution.joins

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.TokenNode.EMPTY_CHAR
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator}

/**
 * This tree does not support null value.
 *
 * @param key
 * @param children
 * @param valuesRef
 * @tparam V
 */
case class TokenNode[V](
    var key: Char,
    var children: HashMap[Char, TokenNode[V]] = HashMap[Char, TokenNode[V]](),
    var valuesRef: ArrayBuffer[V] = null) {

  // for serializer
  def this() = {
    this(EMPTY_CHAR)
  }

  def debugString: String = {
    debugString("")
  }

  def debugString(ident: String): String = {
    val valueRefString =
      if (valuesRef != null) {
        s" --> ${valuesRef.mkString("(", ", ", ")")}"
      } else {
        ""
      }
    val childString =
      if (children.size > 0) {
        children
          .map(tup => tup._2.debugString(ident + "  "))
          .mkString("\n", "\n", "")
      } else {
        ""
      }

    s"${ident}Node{ $key$valueRefString }${childString}"
  }
}

object TokenNode {
  var EMPTY_CHAR: Char = _
}

case class DoubleArrayTrieTree[V](
    var dict: HashMap[Char, Int],
    var base: Array[Int],
    var check: Array[Int],
    var leafMapping: HashMap[Int, ArrayBuffer[V]])
    extends Externalizable
    with KryoSerializable
    with Logging {

  def this() = {
    this(HashMap[Char, Int](), Array[Int](0), Array[Int](0), HashMap[Int, ArrayBuffer[V]]())
  }

  def debugString: String = {
    val sortedDict = dict.toArray.sortBy(_._2)
    val dictString =
      s"""${sortedDict.map(_._2).mkString("[", "\t", "]")}
         |${sortedDict.map(_._1).mkString("[", "\t", "]")}""".stripMargin
    s"""Dict:
       |${dictString}
       |Base:   ${base.mkString("[", "\t", "]")}
       |Check:  ${check.mkString("[", "\t", "]")}
       |Leaf:   $leafMapping
       |""".stripMargin
  }

  def doMatch(input: String): ArrayBuffer[V] = {
    val matchedSet = HashSet[Int]()
    val res = ArrayBuffer[V]()
    var i = 0
    val len = input.length
    while (i < len) {
      var j = i
      var stopped = false
      var pos = 0

      while (j < len && !stopped) {
        if (dict.contains(input(j))) {
          val newPos = pos + Math.abs(base(pos)) + dict(input(j))
          if (newPos >= base.length || base(newPos) == 0 || check(newPos) != pos) {
            stopped = true
          } else {
            if (base(newPos) < 0 && !matchedSet.contains(newPos)) {
              res ++= leafMapping(newPos)
              matchedSet += newPos
            }
            j = j + 1
            pos = newPos
          }
        } else {
          stopped = true
        }
      }
      i = i + 1
    }
    res
  }

  override def hashCode(): Int = {
    dict.hashCode() + 31 * (31 + base.hashCode()) + 31 * (31 + check.hashCode())
  }

  override def equals(other: Any): Boolean = other match {
    case that: DoubleArrayTrieTree[_] =>
      dict.size == that.dict.size && (dict.keySet -- that.dict.keySet).isEmpty &&
        (base diff that.base).isEmpty && (check diff that.check).isEmpty &&
        leafMapping.size == that.leafMapping.size &&
        (leafMapping.keySet -- that.leafMapping.keySet).isEmpty

    case _ => false
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(dict.size)
    val dictArray = dict.toArray
    dictArray.map(_._1).foreach(out.writeChar(_))
    dictArray.map(_._2).foreach(out.writeInt(_))
    out.writeInt(base.length)
    base.foreach(out.writeInt(_))
    check.foreach(out.writeInt(_))

    val leafArray = leafMapping.toArray
    out.writeInt(leafArray.size)
    leafArray.map(_._1).foreach(out.writeInt(_))
    leafArray
      .map(_._2)
      .foreach(buf => {
        out.writeInt(buf.size)
        buf.foreach(out.writeObject(_))
      })
  }

  override def readExternal(in: ObjectInput): Unit = {
    val dictSize = in.readInt()
    val dictKeys = Array.fill(dictSize)(in.readChar())
    val dictValues = Array.fill(dictSize)(in.readInt())
    dict = HashMap[Char, Int]() ++ dictKeys.zip(dictValues).toMap
    val baseSize = in.readInt()
    base = Array.fill(baseSize)(in.readInt())
    check = Array.fill(baseSize)(in.readInt())
    val leafSize = in.readInt()
    val leafMappingKeys = Array.fill(leafSize)(in.readInt())
    val leafMappingValues: Array[ArrayBuffer[V]] = Array.fill(leafSize)({
      val buf = new ArrayBuffer[V]()
      Range(0, in.readInt()).foreach(_ => buf += in.readObject().asInstanceOf[V])
      buf
    })
    leafMapping = HashMap[Int, ArrayBuffer[V]]() ++ leafMappingKeys.zip(leafMappingValues).toMap
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    out.writeInt(dict.size)
    val dictArray = dict.toArray
    dictArray.map(_._1).foreach(out.writeChar(_))
    dictArray.map(_._2).foreach(out.writeInt(_))
    out.writeInt(base.length)
    base.foreach(out.writeInt(_))
    check.foreach(out.writeInt(_))

    val leafArray = leafMapping.toArray
    out.writeInt(leafArray.size)
    leafArray.map(_._1).foreach(out.writeInt(_))
    leafArray
      .map(_._2)
      .foreach(buf => {
        out.writeInt(buf.size)
        buf.foreach(kryo.writeClassAndObject(out, _))
      })
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    val dictSize = in.readInt()
    val dictKeys = Array.fill(dictSize)(in.readChar())
    val dictValues = Array.fill(dictSize)(in.readInt())
    dict = HashMap[Char, Int]() ++ dictKeys.zip(dictValues).toMap
    val baseSize = in.readInt()
    base = Array.fill(baseSize)(in.readInt())
    check = Array.fill(baseSize)(in.readInt())
    val leafSize = in.readInt()
    val leafMappingKeys = Array.fill(leafSize)(in.readInt())
    val leafMappingValues: Array[ArrayBuffer[V]] = Array.fill(leafSize)({
      val buf = new ArrayBuffer[V]()
      Range(0, in.readInt()).foreach(_ => buf += kryo.readClassAndObject(in).asInstanceOf[V])
      buf
    })
    leafMapping = HashMap[Int, ArrayBuffer[V]]() ++ leafMappingKeys.zip(leafMappingValues).toMap
  }
}

class DoubleArrayTreeBuilder[V] extends Logging {

  val header = new TokenNode[V](EMPTY_CHAR)

  def compile(input: String, value: V): DoubleArrayTreeBuilder[V] = {
    var curNode = header
    var i = 0
    val len = input.length
    while (i < len) {
      val ch = input(i)
      curNode = curNode.children.getOrElseUpdate(ch, TokenNode(ch))
      i += 1
    }

    if (curNode.valuesRef == null) {
      curNode.valuesRef = ArrayBuffer[V]()
    }
    curNode.valuesRef += value
    this
  }

  /**
   * encode all chars into a array
   */
  def encodeChars[V](header: TokenNode[V]): HashMap[Char, Int] = {
    val charMapping = HashMap[Char, Int]()
    val buf = ArrayBuffer[TokenNode[V]](header)
    while (buf.nonEmpty) {
      val node = buf.last
      buf.remove(buf.length - 1)
      buf ++= node.children.values

      if (node != header && !charMapping.contains(node.key)) {
        charMapping += node.key -> charMapping.size
      }
    }
    charMapping
  }

  def builder(): DoubleArrayTrieTree[V] = {
    val startTime = System.currentTimeMillis()
    val charMapping = encodeChars(header)
    var base = Array.fill(charMapping.size * 2)(0)
    var check = Array.fill(charMapping.size * 2)(0)
    var nextCheckPos = 0
    val leafMapping = HashMap[Int, ArrayBuffer[V]]()

    val buf = ArrayBuffer[(Int, TokenNode[V])](0 -> header)
    base(0) = 1

    def resize(newSize: Int): Unit = {
      val len = Math.min(base.length, newSize)
      logInfo(s"Increase double array tree size from ${base.length} to ${newSize}")
      val newBase = new Array[Int](newSize)
      Array.copy(base, 0, newBase, 0, len)
      base = newBase
      val newCheck = new Array[Int](newSize)
      Array.copy(check, 0, newCheck, 0, len)
      check = newCheck
    }

    while (buf.nonEmpty) {
      val (position, node) = buf.head
      buf.remove(0)
      // select i which will not conflicts with current bases
      val idToChild = node.children.map { case (key, child) => charMapping(key) -> child }
      var i = Math.max(nextCheckPos - position, 0)
      var conflict = true
      var conflictPosNum = 0
      while (conflict) {
        conflict = false
        i = i + 1
        if (base(i) == 0) {
          conflictPosNum = conflictPosNum + 1
        }
        for (id <- idToChild.keySet if !conflict) {
          // Resize if needed
          if (position + i + id >= base.length) {
            resize(base.length * 2)

          }
          if (base(position + i + id) != 0) {
            conflict = true
          }
        }
      }
      if (1.0 * i / node.children.size > 1) {
        nextCheckPos = position + i
      }

      base(position) = i * base(position) // keep leaf flag(1 or -1)
      idToChild.foreach { case (id, child) =>
        val childPosition = position + i + id
        if (child.valuesRef == null) {
          base(childPosition) = 1
        } else {
          base(childPosition) = -1
          leafMapping += childPosition -> child.valuesRef
        }
        check(childPosition) = position
        if (child.children.nonEmpty) {
          // add next level nodes
          buf += childPosition -> child
        }
      }
      logDebug(s"""AddNode [${{ node.children.map(_._1).mkString(", ") }}]
                  |Index\t${Range(0, base.length).mkString("\t")}
                  |Base\t${base.mkString("\t")}
                  |Check\t${check.mkString("\t")}
                  |""".stripMargin)
    }

    var j = base.length
    while (base(j - 1) == 0) { // base(0) always == 1
      j = j - 1
    }
    resize(j)
    logDebug(
      s"Build DoubleArrayTrieTree from trieTree in ${System.currentTimeMillis() - startTime} ms")
    DoubleArrayTrieTree(charMapping, base, check, leafMapping)
  }
}

/**
 * TODO Here can be optimized in broadcast in executor size PR.
 * @param tree
 * @param estimatedSize
 * @param rowNumber
 */
case class TreeRelation(
    tree: DoubleArrayTrieTree[InternalRow],
    estimatedSize: Long,
    rowNumber: Int)
    extends KnownSizeEstimation
    with Serializable

case class TokenTreeBroadcastMode(
    key: Seq[Expression],
    checkWildcards: Boolean,
    isNullAware: Boolean = false)
    extends BroadcastMode
    with Logging {

  override def transform(rows: Array[InternalRow]): Option[TreeRelation] = {
    transform(rows.iterator, Some(rows.length))
  }

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): Option[TreeRelation] = {
    val buildStart = System.currentTimeMillis()
    val keyGenerator = UnsafeProjection.create(canonicalized.key)

    var rowNumber: Int = 0
    var validRelation = true
    val rowAndPatterns = rows
      .filter(keyGenerator(_).getUTF8String(0) != null)
      .map(row => {
        rowNumber = rowNumber + 1
        val patternKey = keyGenerator(row).getString(0)
        if (checkWildcards) {
          if (patternKey.contains('%') || patternKey.contains('_')) {
            validRelation = false
          }
        }
        row -> patternKey
      })
      .toArray

    if (validRelation) {
      val builder =
        rowAndPatterns.foldLeft(new DoubleArrayTreeBuilder[InternalRow]()) { (builder, tup) =>
          builder.compile(tup._2, tup._1)
        }

      val dat = builder.builder()
      val elapsedTime = System.currentTimeMillis() - buildStart
      val estimatedSize = SizeEstimator.estimate(dat)
      logInfo(
        s"Build contains join trie tree(${estimatedSize} Bytes)" +
          s" in $elapsedTime ms, origin rows number: ${rowNumber}")
      Some(TreeRelation(dat, estimatedSize, rowNumber))
    } else {
      None
    }
  }

  override lazy val canonicalized: TokenTreeBroadcastMode = {
    this.copy(key = key.map(_.canonicalized))
  }
}

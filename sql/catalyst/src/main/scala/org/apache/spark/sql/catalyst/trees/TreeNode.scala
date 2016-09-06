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

package org.apache.spark.sql.catalyst.trees

import java.util.UUID

import scala.collection.Map
import scala.collection.mutable.Stack
import scala.reflect.ClassTag

import org.apache.commons.lang3.ClassUtils
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.ScalaReflection._
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/** Used by [[TreeNode.getNodeNumbered]] when traversing the tree for a given number */
private class MutableInt(var i: Int)

case class Origin(
  line: Option[Int] = None,
  startPosition: Option[Int] = None)

/**
 * Provides a location for TreeNodes to ask about the context of their origin.  For example, which
 * line of code is currently being parsed.
 */
object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int): Unit = {
    value.set(
      value.get.copy(line = Some(line), startPosition = Some(start)))
  }

  def withOrigin[A](o: Origin)(f: => A): A = {
    set(o)
    val ret = try f finally { reset() }
    reset()
    ret
  }
}

// scalastyle:off
abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product {
// scalastyle:on
  self: BaseType =>

  val origin: Origin = CurrentOrigin.get

  /**
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   */
  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  private lazy val _hashCode: Int = scala.util.hashing.MurmurHash3.productHash(this)
  override def hashCode(): Int = _hashCode

  /**
   * Faster version of equality which short-circuits when two treeNodes are the same instance.
   * We don't just override Object.equals, as doing so prevents the scala compiler from
   * generating case class `equals` methods
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  /**
   * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
   */
  def find(f: BaseType => Boolean): Option[BaseType] = if (f(this)) {
    Some(this)
  } else {
    children.foldLeft(Option.empty[BaseType]) { (l, r) => l.orElse(r.find(f)) }
  }

  /**
   * Runs the given function on this node and then recursively on [[children]].
   * @param f the function to be applied to each node in the tree.
   */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
   * Runs the given function recursively on [[children]] then on this node.
   * @param f the function to be applied to each node in the tree.
   */
  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
   * Returns a Seq containing the result of applying the given function to each
   * node in this tree in a preorder traversal.
   * @param f the function to be applied.
   */
  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret
  }

  /**
   * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
   * resulting collections.
   */
  def flatMap[A](f: BaseType => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))
    ret
  }

  /**
   * Returns a Seq containing the result of applying a partial function to all elements in this
   * tree on which the function is defined.
   */
  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret
  }

  /**
   * Finds and returns the first [[TreeNode]] of the tree for which the given partial function
   * is defined (pre-order), and applies the partial function to it.
   */
  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift
    lifted(this).orElse {
      children.foldLeft(Option.empty[B]) { (l, r) => l.orElse(r.collectFirst(pf)) }
    }
  }

  /**
   * Efficient alternative to `productIterator.map(f).toArray`.
   */
  protected def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
    val arr = Array.ofDim[B](productArity)
    var i = 0
    while (i < arr.length) {
      arr(i) = f(productElement(i))
      i += 1
    }
    arr
  }

  /**
   * Returns a copy of this node where `f` has been applied to all the nodes children.
   */
  def mapChildren(f: BaseType => BaseType): BaseType = {
    var changed = false
    val newArgs = mapProductIterator {
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (newChild fastEquals arg) {
          arg
        } else {
          changed = true
          newChild
        }
      case nonChild: AnyRef => nonChild
      case null => null
    }
    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node with the children replaced.
   * TODO: Validate somewhere (in debug mode?) that children are ordered correctly.
   */
  def withNewChildren(newChildren: Seq[BaseType]): BaseType = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    var changed = false
    val remainingNewChildren = newChildren.toBuffer
    val remainingOldChildren = children.toBuffer
    val newArgs = mapProductIterator {
      case s: StructType => s // Don't convert struct types to some other type of Seq[StructField]
      // Handle Seq[TreeNode] in TreeNode parameters.
      case s: Seq[_] => s.map {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = remainingNewChildren.remove(0)
          val oldChild = remainingOldChildren.remove(0)
          if (newChild fastEquals oldChild) {
            oldChild
          } else {
            changed = true
            newChild
          }
        case nonChild: AnyRef => nonChild
        case null => null
      }
      case m: Map[_, _] => m.mapValues {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = remainingNewChildren.remove(0)
          val oldChild = remainingOldChildren.remove(0)
          if (newChild fastEquals oldChild) {
            oldChild
          } else {
            changed = true
            newChild
          }
        case nonChild: AnyRef => nonChild
        case null => null
      }.view.force // `mapValues` is lazy and we need to force it to materialize
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = remainingNewChildren.remove(0)
        val oldChild = remainingOldChildren.remove(0)
        if (newChild fastEquals oldChild) {
          oldChild
        } else {
          changed = true
          newChild
        }
      case nonChild: AnyRef => nonChild
      case null => null
    }

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   *
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   *
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse(this, identity[BaseType])
    }

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
      transformChildren(rule, (t, r) => t.transformDown(r))
    } else {
      afterRule.transformChildren(rule, (t, r) => t.transformDown(r))
    }
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
   *
   * @param rule the function use to transform this nodes children
   */
  def transformUp(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRuleOnChildren = transformChildren(rule, (t, r) => t.transformUp(r))
    if (this fastEquals afterRuleOnChildren) {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(this, identity[BaseType])
      }
    } else {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
      }
    }
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to all the children of
   * this node.  When `rule` does not apply to a given node it is left unchanged.
   * @param rule the function used to transform this nodes children
   */
  protected def transformChildren(
      rule: PartialFunction[BaseType, BaseType],
      nextOperation: (BaseType, PartialFunction[BaseType, BaseType]) => BaseType): BaseType = {
    if (children.nonEmpty) {
      var changed = false
      val newArgs = mapProductIterator {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case Some(arg: TreeNode[_]) if containsChild(arg) =>
          val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)
          if (!(newChild fastEquals arg)) {
            changed = true
            Some(newChild)
          } else {
            Some(arg)
          }
        case m: Map[_, _] => m.mapValues {
          case arg: TreeNode[_] if containsChild(arg) =>
            val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)
            if (!(newChild fastEquals arg)) {
              changed = true
              newChild
            } else {
              arg
            }
          case other => other
        }.view.force // `mapValues` is lazy and we need to force it to materialize
        case d: DataType => d // Avoid unpacking Structs
        case args: Traversable[_] => args.map {
          case arg: TreeNode[_] if containsChild(arg) =>
            val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)
            if (!(newChild fastEquals arg)) {
              changed = true
              newChild
            } else {
              arg
            }
          case tuple@(arg1: TreeNode[_], arg2: TreeNode[_]) =>
            val newChild1 = nextOperation(arg1.asInstanceOf[BaseType], rule)
            val newChild2 = nextOperation(arg2.asInstanceOf[BaseType], rule)
            if (!(newChild1 fastEquals arg1) || !(newChild2 fastEquals arg2)) {
              changed = true
              (newChild1, newChild2)
            } else {
              tuple
            }
          case other => other
        }
        case nonChild: AnyRef => nonChild
        case null => null
      }
      if (changed) makeCopy(newArgs) else this
    } else {
      this
    }
  }

  /**
   * Args to the constructor that should be copied, but not transformed.
   * These are appended to the transformed args automatically by makeCopy
   * @return
   */
  protected def otherCopyArgs: Seq[AnyRef] = Nil

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   * @param newArgs the new product arguments.
   */
  def makeCopy(newArgs: Array[AnyRef]): BaseType = attachTree(this, "makeCopy") {
    // Skip no-arg constructors that are just there for kryo.
    val ctors = getClass.getConstructors.filter(_.getParameterTypes.size != 0)
    if (ctors.isEmpty) {
      sys.error(s"No valid constructor for $nodeName")
    }
    val allArgs: Array[AnyRef] = if (otherCopyArgs.isEmpty) {
      newArgs
    } else {
      newArgs ++ otherCopyArgs
    }
    val defaultCtor = ctors.find { ctor =>
      if (ctor.getParameterTypes.length != allArgs.length) {
        false
      } else if (allArgs.contains(null)) {
        // if there is a `null`, we can't figure out the class, therefore we should just fallback
        // to older heuristic
        false
      } else {
        val argsArray: Array[Class[_]] = allArgs.map(_.getClass)
        ClassUtils.isAssignable(argsArray, ctor.getParameterTypes, true /* autoboxing */)
      }
    }.getOrElse(ctors.maxBy(_.getParameterTypes.length)) // fall back to older heuristic

    try {
      CurrentOrigin.withOrigin(origin) {
        defaultCtor.newInstance(allArgs.toArray: _*).asInstanceOf[BaseType]
      }
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw new TreeNodeException(
          this,
          s"""
             |Failed to copy node.
             |Is otherCopyArgs specified correctly for $nodeName.
             |Exception message: ${e.getMessage}
             |ctor: $defaultCtor?
             |types: ${newArgs.map(_.getClass).mkString(", ")}
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
    }
  }

  /**
   * Returns the name of this type of TreeNode.  Defaults to the class name.
   * Note that we remove the "Exec" suffix for physical operators here.
   */
  def nodeName: String = getClass.getSimpleName.replaceAll("Exec$", "")

  /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
   */
  protected def stringArgs: Iterator[Any] = productIterator

  private lazy val allChildren: Set[TreeNode[_]] = (children ++ innerChildren).toSet[TreeNode[_]]

  /** Returns a string representing the arguments to this node, minus any children */
  def argString: String = stringArgs.flatMap {
    case tn: TreeNode[_] if allChildren.contains(tn) => Nil
    case Some(tn: TreeNode[_]) if allChildren.contains(tn) => Nil
    case Some(tn: TreeNode[_]) => tn.simpleString :: Nil
    case tn: TreeNode[_] => tn.simpleString :: Nil
    case seq: Seq[Any] if seq.toSet.subsetOf(allChildren.asInstanceOf[Set[Any]]) => Nil
    case iter: Iterable[_] if iter.isEmpty => Nil
    case seq: Seq[_] => Utils.truncatedString(seq, "[", ", ", "]") :: Nil
    case set: Set[_] => Utils.truncatedString(set.toSeq, "{", ", ", "}") :: Nil
    case array: Array[_] if array.isEmpty => Nil
    case array: Array[_] => Utils.truncatedString(array, "[", ", ", "]") :: Nil
    case null => Nil
    case None => Nil
    case Some(null) => Nil
    case Some(any) => any :: Nil
    case other => other :: Nil
  }.mkString(", ")

  /** ONE line description of this node. */
  def simpleString: String = s"$nodeName $argString".trim

  /** ONE line description of this node with more information */
  def verboseString: String

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree */
  def treeString: String = treeString(verbose = true)

  def treeString(verbose: Boolean): String = {
    generateTreeString(0, Nil, new StringBuilder, verbose).toString
  }

  /**
   * Returns a string representation of the nodes in this tree, where each operator is numbered.
   * The numbers can be used with [[trees.TreeNode.apply apply]] to easily access specific subtrees.
   */
  def numberedTreeString: String =
    treeString.split("\n").zipWithIndex.map { case (line, i) => f"$i%02d $line" }.mkString("\n")

  /**
   * Returns the tree node at the specified number.
   * Numbers for each node can be found in the [[numberedTreeString]].
   */
  def apply(number: Int): BaseType = getNodeNumbered(new MutableInt(number))

  protected def getNodeNumbered(number: MutableInt): BaseType = {
    if (number.i < 0) {
      null.asInstanceOf[BaseType]
    } else if (number.i == 0) {
      this
    } else {
      number.i -= 1
      children.map(_.getNodeNumbered(number)).find(_ != null).getOrElse(null.asInstanceOf[BaseType])
    }
  }

  /**
   * All the nodes that should be shown as a inner nested tree of this node.
   * For example, this can be used to show sub-queries.
   */
  protected def innerChildren: Seq[TreeNode[_]] = Seq.empty

  /**
   * Appends the string represent of this node and its children to the given StringBuilder.
   *
   * The `i`-th element in `lastChildren` indicates whether the ancestor of the current node at
   * depth `i + 1` is the last child of its own parent node.  The depth of the root node is 0, and
   * `lastChildren` for the root node should be empty.
   */
  def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = ""): StringBuilder = {
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        val prefixFragment = if (isLast) "   " else ":  "
        builder.append(prefixFragment)
      }

      val branch = if (lastChildren.last) "+- " else ":- "
      builder.append(branch)
    }

    builder.append(prefix)
    val headline = if (verbose) verboseString else simpleString
    builder.append(headline)
    builder.append("\n")

    if (innerChildren.nonEmpty) {
      innerChildren.init.foreach(_.generateTreeString(
        depth + 2, lastChildren :+ children.isEmpty :+ false, builder, verbose))
      innerChildren.last.generateTreeString(
        depth + 2, lastChildren :+ children.isEmpty :+ true, builder, verbose)
    }

    if (children.nonEmpty) {
      children.init.foreach(
        _.generateTreeString(depth + 1, lastChildren :+ false, builder, verbose, prefix))
      children.last.generateTreeString(depth + 1, lastChildren :+ true, builder, verbose, prefix)
    }

    builder
  }

  /**
   * Returns a 'scala code' representation of this `TreeNode` and its children.  Intended for use
   * when debugging where the prettier toString function is obfuscating the actual structure. In the
   * case of 'pure' `TreeNodes` that only contain primitives and other TreeNodes, the result can be
   * pasted in the REPL to build an equivalent Tree.
   */
  def asCode: String = {
    val args = productIterator.map {
      case tn: TreeNode[_] => tn.asCode
      case s: String => "\"" + s + "\""
      case other => other.toString
    }
    s"$nodeName(${args.mkString(",")})"
  }

  def toJSON: String = compact(render(jsonValue))

  def prettyJson: String = pretty(render(jsonValue))

  private def jsonValue: JValue = {
    val jsonValues = scala.collection.mutable.ArrayBuffer.empty[JValue]

    def collectJsonValue(tn: BaseType): Unit = {
      val jsonFields = ("class" -> JString(tn.getClass.getName)) ::
        ("num-children" -> JInt(tn.children.length)) :: tn.jsonFields
      jsonValues += JObject(jsonFields)
      tn.children.foreach(collectJsonValue)
    }

    collectJsonValue(this)
    jsonValues
  }

  protected def jsonFields: List[JField] = {
    val fieldNames = getConstructorParameterNames(getClass)
    val fieldValues = productIterator.toSeq ++ otherCopyArgs
    assert(fieldNames.length == fieldValues.length, s"${getClass.getSimpleName} fields: " +
      fieldNames.mkString(", ") + s", values: " + fieldValues.map(_.toString).mkString(", "))

    fieldNames.zip(fieldValues).map {
      // If the field value is a child, then use an int to encode it, represents the index of
      // this child in all children.
      case (name, value: TreeNode[_]) if containsChild(value) =>
        name -> JInt(children.indexOf(value))
      case (name, value: Seq[BaseType]) if value.toSet.subsetOf(containsChild) =>
        name -> JArray(
          value.map(v => JInt(children.indexOf(v.asInstanceOf[TreeNode[_]]))).toList
        )
      case (name, value) => name -> parseToJson(value)
    }.toList
  }

  private def parseToJson(obj: Any): JValue = obj match {
    case b: Boolean => JBool(b)
    case b: Byte => JInt(b.toInt)
    case s: Short => JInt(s.toInt)
    case i: Int => JInt(i)
    case l: Long => JInt(l)
    case f: Float => JDouble(f)
    case d: Double => JDouble(d)
    case b: BigInt => JInt(b)
    case null => JNull
    case s: String => JString(s)
    case u: UUID => JString(u.toString)
    case dt: DataType => dt.jsonValue
    // SPARK-17356: In usage of mllib, Metadata may store a huge vector of data, transforming
    // it to JSON may trigger OutOfMemoryError.
    case m: Metadata => Metadata.empty.jsonValue
    case s: StorageLevel =>
      ("useDisk" -> s.useDisk) ~ ("useMemory" -> s.useMemory) ~ ("useOffHeap" -> s.useOffHeap) ~
        ("deserialized" -> s.deserialized) ~ ("replication" -> s.replication)
    case n: TreeNode[_] => n.jsonValue
    case o: Option[_] => o.map(parseToJson)
    case t: Seq[_] => JArray(t.map(parseToJson).toList)
    case m: Map[_, _] =>
      val fields = m.toList.map { case (k: String, v) => (k, parseToJson(v)) }
      JObject(fields)
    case r: RDD[_] => JNothing
    // if it's a scala object, we can simply keep the full class path.
    // TODO: currently if the class name ends with "$", we think it's a scala object, there is
    // probably a better way to check it.
    case obj if obj.getClass.getName.endsWith("$") => "object" -> obj.getClass.getName
    // returns null if the product type doesn't have a primary constructor, e.g. HiveFunctionWrapper
    case p: Product => try {
      val fieldNames = getConstructorParameterNames(p.getClass)
      val fieldValues = p.productIterator.toSeq
      assert(fieldNames.length == fieldValues.length)
      ("product-class" -> JString(p.getClass.getName)) :: fieldNames.zip(fieldValues).map {
        case (name, value) => name -> parseToJson(value)
      }.toList
    } catch {
      case _: RuntimeException => null
    }
    case _ => JNull
  }
}

object TreeNode {
  def fromJSON[BaseType <: TreeNode[BaseType]](json: String, sc: SparkContext): BaseType = {
    val jsonAST = parse(json)
    assert(jsonAST.isInstanceOf[JArray])
    reconstruct(jsonAST.asInstanceOf[JArray], sc).asInstanceOf[BaseType]
  }

  private def reconstruct(treeNodeJson: JArray, sc: SparkContext): TreeNode[_] = {
    assert(treeNodeJson.arr.forall(_.isInstanceOf[JObject]))
    val jsonNodes = Stack(treeNodeJson.arr.map(_.asInstanceOf[JObject]): _*)

    def parseNextNode(): TreeNode[_] = {
      val nextNode = jsonNodes.pop()

      val cls = Utils.classForName((nextNode \ "class").asInstanceOf[JString].s)
      if (cls == classOf[Literal]) {
        Literal.fromJSON(nextNode)
      } else if (cls.getName.endsWith("$")) {
        cls.getField("MODULE$").get(cls).asInstanceOf[TreeNode[_]]
      } else {
        val numChildren = (nextNode \ "num-children").asInstanceOf[JInt].num.toInt

        val children: Seq[TreeNode[_]] = (1 to numChildren).map(_ => parseNextNode())
        val fields = getConstructorParameters(cls)

        val parameters: Array[AnyRef] = fields.map {
          case (fieldName, fieldType) =>
            parseFromJson(nextNode \ fieldName, fieldType, children, sc)
        }.toArray

        val maybeCtor = cls.getConstructors.find { p =>
          val expectedTypes = p.getParameterTypes
          expectedTypes.length == fields.length && expectedTypes.zip(fields.map(_._2)).forall {
            case (cls, tpe) => cls == getClassFromType(tpe)
          }
        }
        if (maybeCtor.isEmpty) {
          sys.error(s"No valid constructor for ${cls.getName}")
        } else {
          try {
            maybeCtor.get.newInstance(parameters: _*).asInstanceOf[TreeNode[_]]
          } catch {
            case e: java.lang.IllegalArgumentException =>
              throw new RuntimeException(
                s"""
                  |Failed to construct tree node: ${cls.getName}
                  |ctor: ${maybeCtor.get}
                  |types: ${parameters.map(_.getClass).mkString(", ")}
                  |args: ${parameters.mkString(", ")}
                """.stripMargin, e)
          }
        }
      }
    }

    parseNextNode()
  }

  import universe._

  private def parseFromJson(
      value: JValue,
      expectedType: Type,
      children: Seq[TreeNode[_]],
      sc: SparkContext): AnyRef = ScalaReflectionLock.synchronized {
    if (value == JNull) return null

    expectedType match {
      case t if t <:< definitions.BooleanTpe =>
        value.asInstanceOf[JBool].value: java.lang.Boolean
      case t if t <:< definitions.ByteTpe =>
        value.asInstanceOf[JInt].num.toByte: java.lang.Byte
      case t if t <:< definitions.ShortTpe =>
        value.asInstanceOf[JInt].num.toShort: java.lang.Short
      case t if t <:< definitions.IntTpe =>
        value.asInstanceOf[JInt].num.toInt: java.lang.Integer
      case t if t <:< definitions.LongTpe =>
        value.asInstanceOf[JInt].num.toLong: java.lang.Long
      case t if t <:< definitions.FloatTpe =>
        value.asInstanceOf[JDouble].num.toFloat: java.lang.Float
      case t if t <:< definitions.DoubleTpe =>
        value.asInstanceOf[JDouble].num: java.lang.Double

      case t if t <:< localTypeOf[java.lang.Boolean] =>
        value.asInstanceOf[JBool].value: java.lang.Boolean
      case t if t <:< localTypeOf[BigInt] => value.asInstanceOf[JInt].num
      case t if t <:< localTypeOf[java.lang.String] => value.asInstanceOf[JString].s
      case t if t <:< localTypeOf[UUID] => UUID.fromString(value.asInstanceOf[JString].s)
      case t if t <:< localTypeOf[DataType] => DataType.parseDataType(value)
      case t if t <:< localTypeOf[Metadata] => Metadata.fromJObject(value.asInstanceOf[JObject])
      case t if t <:< localTypeOf[StorageLevel] =>
        val JBool(useDisk) = value \ "useDisk"
        val JBool(useMemory) = value \ "useMemory"
        val JBool(useOffHeap) = value \ "useOffHeap"
        val JBool(deserialized) = value \ "deserialized"
        val JInt(replication) = value \ "replication"
        StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication.toInt)
      case t if t <:< localTypeOf[TreeNode[_]] => value match {
        case JInt(i) => children(i.toInt)
        case arr: JArray => reconstruct(arr, sc)
        case _ => throw new RuntimeException(s"$value is not a valid json value for tree node.")
      }
      case t if t <:< localTypeOf[Option[_]] =>
        if (value == JNothing) {
          None
        } else {
          val TypeRef(_, _, Seq(optType)) = t
          Option(parseFromJson(value, optType, children, sc))
        }
      case t if t <:< localTypeOf[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val JArray(elements) = value
        elements.map(parseFromJson(_, elementType, children, sc)).toSeq
      case t if t <:< localTypeOf[Map[_, _]] =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        val JObject(fields) = value
        fields.map {
          case (name, value) => name -> parseFromJson(value, valueType, children, sc)
        }.toMap
      case t if t <:< localTypeOf[RDD[_]] =>
        new EmptyRDD[Any](sc)
      case _ if isScalaObject(value) =>
        val JString(clsName) = value \ "object"
        val cls = Utils.classForName(clsName)
        cls.getField("MODULE$").get(cls)
      case t if t <:< localTypeOf[Product] =>
        val fields = getConstructorParameters(t)
        val clsName = getClassNameFromType(t)
        parseToProduct(clsName, fields, value, children, sc)
      // There maybe some cases that the parameter type signature is not Product but the value is,
      // e.g. `SpecifiedWindowFrame` with type signature `WindowFrame`, handle it here.
      case _ if isScalaProduct(value) =>
        val JString(clsName) = value \ "product-class"
        val fields = getConstructorParameters(Utils.classForName(clsName))
        parseToProduct(clsName, fields, value, children, sc)
      case _ => sys.error(s"Do not support type $expectedType with json $value.")
    }
  }

  private def parseToProduct(
      clsName: String,
      fields: Seq[(String, Type)],
      value: JValue,
      children: Seq[TreeNode[_]],
      sc: SparkContext): AnyRef = {
    val parameters: Array[AnyRef] = fields.map {
      case (fieldName, fieldType) => parseFromJson(value \ fieldName, fieldType, children, sc)
    }.toArray
    val ctor = Utils.classForName(clsName).getConstructors.maxBy(_.getParameterTypes.size)
    ctor.newInstance(parameters: _*).asInstanceOf[AnyRef]
  }

  private def isScalaObject(jValue: JValue): Boolean = (jValue \ "object") match {
    case JString(str) if str.endsWith("$") => true
    case _ => false
  }

  private def isScalaProduct(jValue: JValue): Boolean = (jValue \ "product-class") match {
    case _: JString => true
    case _ => false
  }
}

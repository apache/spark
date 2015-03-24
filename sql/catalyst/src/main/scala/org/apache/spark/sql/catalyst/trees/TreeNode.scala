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

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.types.DataType

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

abstract class TreeNode[BaseType <: TreeNode[BaseType]] {
  self: BaseType with Product =>

  val origin: Origin = CurrentOrigin.get

  /** Returns a Seq of the children of this node */
  def children: Seq[BaseType]

  /**
   * Faster version of equality which short-circuits when two treeNodes are the same instance.
   * We don't just override Object.equals, as doing so prevents the scala compiler from
   * generating case class `equals` methods
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
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
    children.foreach(_.foreach(f))
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
   * Returns a copy of this node where `f` has been applied to all the nodes children.
   */
  def mapChildren(f: BaseType => BaseType): this.type = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if children contains arg =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (newChild fastEquals arg) {
          arg
        } else {
          changed = true
          newChild
        }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node with the children replaced.
   * TODO: Validate somewhere (in debug mode?) that children are ordered correctly.
   */
  def withNewChildren(newChildren: Seq[BaseType]): this.type = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    var changed = false
    val remainingNewChildren = newChildren.toBuffer
    val remainingOldChildren = children.toBuffer
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if children contains arg =>
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
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse(this, identity[BaseType])
    }

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
      transformChildrenDown(rule)
    } else {
      afterRule.transformChildrenDown(rule)
    }
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to all the children of
   * this node.  When `rule` does not apply to a given node it is left unchanged.
   * @param rule the function used to transform this nodes children
   */
  def transformChildrenDown(rule: PartialFunction[BaseType, BaseType]): this.type = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if children contains arg =>
        val newChild = arg.asInstanceOf[BaseType].transformDown(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case Some(arg: TreeNode[_]) if children contains arg =>
        val newChild = arg.asInstanceOf[BaseType].transformDown(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          Some(newChild)
        } else {
          Some(arg)
        }
      case m: Map[_,_] => m
      case d: DataType => d // Avoid unpacking Structs
      case args: Traversable[_] => args.map {
        case arg: TreeNode[_] if children contains arg =>
          val newChild = arg.asInstanceOf[BaseType].transformDown(rule)
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case other => other
      }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
   * @param rule the function use to transform this nodes children
   */
  def transformUp(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRuleOnChildren = transformChildrenUp(rule);
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

  def transformChildrenUp(rule: PartialFunction[BaseType, BaseType]): this.type = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if children contains arg =>
        val newChild = arg.asInstanceOf[BaseType].transformUp(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case Some(arg: TreeNode[_]) if children contains arg =>
        val newChild = arg.asInstanceOf[BaseType].transformUp(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          Some(newChild)
        } else {
          Some(arg)
        }
      case m: Map[_,_] => m
      case d: DataType => d // Avoid unpacking Structs
      case args: Traversable[_] => args.map {
        case arg: TreeNode[_] if children contains arg =>
          val newChild = arg.asInstanceOf[BaseType].transformUp(rule)
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case other => other
      }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    if (changed) makeCopy(newArgs) else this
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
  def makeCopy(newArgs: Array[AnyRef]): this.type = attachTree(this, "makeCopy") {
    val defaultCtor =
      getClass.getConstructors
        .find(_.getParameterTypes.size != 0)
        .headOption
        .getOrElse(sys.error(s"No valid constructor for $nodeName"))

    try {
      CurrentOrigin.withOrigin(origin) {
        // Skip no-arg constructors that are just there for kryo.
        if (otherCopyArgs.isEmpty) {
          defaultCtor.newInstance(newArgs: _*).asInstanceOf[this.type]
        } else {
          defaultCtor.newInstance((newArgs ++ otherCopyArgs).toArray: _*).asInstanceOf[this.type]
        }
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
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
    }
  }

  /** Returns the name of this type of TreeNode.  Defaults to the class name. */
  def nodeName: String = getClass.getSimpleName

  /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
   */
  protected def stringArgs: Iterator[Any] = productIterator

  /** Returns a string representing the arguments to this node, minus any children */
  def argString: String = productIterator.flatMap {
    case tn: TreeNode[_] if children contains tn => Nil
    case tn: TreeNode[_] if tn.toString contains "\n" => s"(${tn.simpleString})" :: Nil
    case seq: Seq[_] => seq.mkString("[", ",", "]") :: Nil
    case set: Set[_] => set.mkString("{", ",", "}") :: Nil
    case other => other :: Nil
  }.mkString(", ")

  /** String representation of this node without any children */
  def simpleString: String = s"$nodeName $argString".trim

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree */
  def treeString: String = generateTreeString(0, new StringBuilder).toString

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

  /** Appends the string represent of this node and its children to the given StringBuilder. */
  protected def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
    builder.append(" " * depth)
    builder.append(simpleString)
    builder.append("\n")
    children.foreach(_.generateTreeString(depth + 1, builder))
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
}

/**
 * A [[TreeNode]] that has two children, [[left]] and [[right]].
 */
trait BinaryNode[BaseType <: TreeNode[BaseType]] {
  def left: BaseType
  def right: BaseType

  def children: Seq[BaseType] = Seq(left, right)
}

/**
 * A [[TreeNode]] with no children.
 */
trait LeafNode[BaseType <: TreeNode[BaseType]] {
  def children: Seq[BaseType] = Nil
}

/**
 * A [[TreeNode]] with a single [[child]].
 */
trait UnaryNode[BaseType <: TreeNode[BaseType]] {
  def child: BaseType
  def children: Seq[BaseType] = child :: Nil
}

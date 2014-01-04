package catalyst
package trees

import errors._

object TreeNode {
  private val currentId = new java.util.concurrent.atomic.AtomicLong
  protected def nextId() = currentId.getAndIncrement()
}

/** Used when traversing the tree for a node at a given depth */
private class MutableInt(var i: Int)

abstract class TreeNode[BaseType <: TreeNode[BaseType]] {
  self: BaseType with Product =>

  /** Returns a Seq of the children of this node */
  def children: Seq[BaseType]

  /**
   * A globally unique id for this specific instance. Not preserved across copies.
   * Unlike [[equals]] [[id]] be used to differentiate distinct but structurally
   * identical branches of a tree.
   */
  val id = TreeNode.nextId()

  /**
   * Returns true if other is the same [[TreeNode]] instance.  Unlike [[equals]] this function will
   * return false for different instances of structurally identical trees.
   */
  def sameInstance(other: TreeNode[_]): Boolean = {
    this.id == other.id
  }

  /**
   * Faster version of equality which short-circuits when two treeNodes are the same instance.
   * We don't just override Object.Equals, as doing so prevents the scala compiler from from
   * generating case class [[equals]] methods.
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    sameInstance(other) || this == other
  }

  /**
   * Runs [[f]] on this node and then recursively on [[children]].
   * @param f the function to be applied to each node in the tree.
   */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
   * Returns a Seq containing the result of applying [[f]] to each
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
   * Returns a copy of this node where [[rule]] has been recursively
   * applied to it and all of its children.  When [[rule]] does not
   * apply to a given node it is left unchanged.
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = rule.applyOrElse(this, identity[BaseType])
    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
      transformChildren(rule)
    } else {
      afterRule.transformChildren(rule)
    }
  }

  /**
   * Returns a copy of this node where [[rule]] has been recursively
   * applied to all the children of this node.  When [[rule]] does not
   * apply to a given node it is left unchanged.
   * @param rule the function use to transform this nodes children
   */
  def transformChildren(rule: PartialFunction[BaseType, BaseType]): this.type = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if (children contains arg) =>
        val newChild = arg.asInstanceOf[BaseType].transform(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case args: Seq[_] => args.map {
        case arg: TreeNode[_] if (children contains arg) =>
          val newChild = arg.asInstanceOf[BaseType].transform(rule)
          if(!(newChild fastEquals arg)) {
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
    if(changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where [[rule]] has been recursively
   * applied first to all of its children and then itself.
   * When [[rule]] does not apply to a given node, it is left unchanged.
   * @param rule the function use to transform this nodes children
   */
  def transformPostOrder(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRuleOnChildren = transformChildrenPostOrder(rule);
    if (this fastEquals afterRuleOnChildren) {
      rule.applyOrElse(this, identity[BaseType])
    } else {
      rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
    }
  }

  def transformChildrenPostOrder(rule: PartialFunction[BaseType, BaseType]): this.type = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if(children contains arg) =>
        val newChild = arg.asInstanceOf[BaseType].transformPostOrder(rule)
        if(!(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case args: Seq[_] => args.map {
        case arg: TreeNode[_] if(children contains arg) =>
          val newChild = arg.asInstanceOf[BaseType].transformPostOrder(rule)
          if(!(newChild fastEquals arg)) {
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
    if(changed) makeCopy(newArgs) else this
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
   * that are not present in the [[productIterator]].
   * @param newArgs the new product arguments.
   */
  def makeCopy(newArgs: Array[AnyRef]): this.type = attachTree(this, "makeCopy") {
    try {
      val defaultCtor = getClass.getConstructors.head
      if (otherCopyArgs.isEmpty) {
        defaultCtor.newInstance(newArgs: _*).asInstanceOf[this.type]
      } else {
        defaultCtor.newInstance((newArgs ++ otherCopyArgs).toArray: _*).asInstanceOf[this.type]
      }
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw new OptimizationException(
          this, s"Failed to copy node.  Is otherCopyArgs specified correctly for $nodeName?")
    }
  }

  /** Returns the name of this type of TreeNode.  Defaults to the class name. */
  def nodeName = getClass.getSimpleName

  /**
   * The arguments that should be included in the arg string.  Defaults to the [[productIterator]].
   */
  protected def stringArgs = productIterator

  /** Returns a string representing the arguments to this node, minus any children */
  def argString: String = productIterator.flatMap {
    case tn: TreeNode[_] if children contains tn => Nil
    case tn: TreeNode[_] if tn.toString contains "\n" => s"(${tn.simpleString})" :: Nil
    case seq: Seq[_] => seq.mkString("{", ",", "}") :: Nil
    case other => other :: Nil
  }.mkString(", ")

  /** String representation of this node without any children */
  def simpleString = s"$nodeName $argString"

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree */
  def treeString = generateTreeString(0, new StringBuilder).toString

  /**
   * Returns a string representation of the nodes in this tree, where each operator is numbered.
   * The numbers can be used with [[apply]] to easily access specific subtrees.
   */
  def numberedTreeString =
    treeString.split("\n").zipWithIndex.map { case (line, i) => f"$i%02d $line" }.mkString("\n")

  def apply(depth: Int): BaseType = getNodeAtDepth(new MutableInt(depth))

  protected def getNodeAtDepth(depth: MutableInt): BaseType = {
    if (depth.i < 0) {
      null.asInstanceOf[BaseType]
    } else if (depth.i == 0) {
      this
    } else {
      depth.i -= 1
      children.map(_.getNodeAtDepth(depth)).find(_ != null).getOrElse(sys.error("Invalid depth"))
    }
  }

  /** Appends the string represent of this node and its children to [[builder]]. */
  protected def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
    builder.append(" " * depth)
    builder.append(simpleString)
    builder.append("\n")
    children.foreach(_.generateTreeString(depth + 1, builder))
    builder
  }
}

/**
 * A [[TreeNode]] that has two children, [[left]] and [[right]].
 */
trait BinaryNode[BaseType <: TreeNode[BaseType]] {
  def left: BaseType
  def right: BaseType

  def children = Seq(left, right)
}

/**
 * A [[TreeNode]] with no children.
 */
trait LeafNode[BaseType <: TreeNode[BaseType]] {
  def children = Nil
}

/**
 * A [[TreeNode]] with a single [[child]].
 */
trait UnaryNode[BaseType <: TreeNode[BaseType]] {
  def child: BaseType
  def children = child :: Nil
}

package catalyst
package trees

object TreeNode {
  private val currentId = new java.util.concurrent.atomic.AtomicLong
  protected def nextId() = currentId.getAndIncrement()
}

abstract class TreeNode[BaseType <: TreeNode[BaseType]] {
  self: BaseType with Product =>

  /** Returns a Seq of the children of this node */
  def children: Seq[BaseType]

  /**
   * A globally unique id for this specific instance. Not preserved across copies.
   * Unlike [[equals]] [[id]] be used to differentiate distinct but stucturally
   * identical branches of a tree.
   */
  val id = TreeNode.nextId()

  /**
   * Faster version of equality which short-circuits when two treeNodes have the same id.
   * We don't just override Object.Equals, as doing so prevents the scala compiler from
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.id == other.id || this == other
  }

  /**
   * Runs [[f]] on this node and then recursively on [[children]].
   * @param f the function to be applied to each node in the tree.
   */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(f)
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
   * Returns a copy of this node where [[rule]] has been recursively
   * applied to it and all of its children.  When [[rule]] does not
   * apply to a given node it is left unchanged.
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = rule.applyOrElse(this, identity[BaseType])
    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if(this.id == afterRule.id || this == afterRule)
      transformChildren(rule)
    else
      afterRule.transformChildren(rule)
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
      case arg: TreeNode[_] if(children contains arg) =>
          val newChild = arg.asInstanceOf[BaseType].transform(rule)
          if(newChild.id != arg.id && newChild != arg) {
            changed = true
            newChild
          } else {
            arg
          }
      case nonChild: AnyRef => nonChild
    }.toArray
    if(changed) makeCopy(newArgs) else this
  }

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the [[productIterator]].
   * @param newArgs the new product arguments.
   */
  protected def makeCopy(newArgs: Array[AnyRef]): this.type =
    getClass.getConstructors.head.newInstance(newArgs: _*).asInstanceOf[this.type]

  /** Returns the name of this type of TreeNode.  Defaults to the class name. */
  def nodeName = getClass.getSimpleName

  /** Returns a string representing the arguments to this node, minus any children */
  def argString = productIterator.flatMap {
    case tn: TreeNode[_] if children contains tn => Nil
    case seq: Seq[_] => seq.mkString("{", ",", "}") :: Nil
    case other => other :: Nil
  }.mkString(", ")

  /** String representation of this node without any children */
  def simpleString = s"$nodeName $argString"

  override def toString(): String = generateTreeString(0, new StringBuilder).toString

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

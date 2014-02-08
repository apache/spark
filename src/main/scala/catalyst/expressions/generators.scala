package catalyst
package expressions

import catalyst.types._

/**
 * An expression that produces zero or more rows given a single input row.
 *
 * Generators produce multiple output rows instead of a single value like other expressions,
 * and thus they must have a schema to associate with the rows that are output.
 *
 * However, unlike row producing relational operators, which are either leaves or determine their
 * output schema functionally from their input, generators can contain other expressions that
 * might result in their modification by rules.  This structure means that they might be copied
 * multiple times after first determining their output schema. If a new output schema is created for
 * each copy references up the tree might be rendered invalid. As a result generators must
 * instead define a function `makeOutput` which is called only once when the schema is first
 * requested.  The attributes produced by this function will be automatically copied anytime rules
 * result in changes to the Generator or its children.
 */
abstract class Generator extends Expression with (Row => TraversableOnce[Row]) {
  self: Product =>

  lazy val dataType =
    ArrayType(StructType(output.map(a => StructField(a.name, a.dataType, a.nullable))))

  def nullable = false

  def references = children.flatMap(_.references).toSet

  /**
   * Should be overridden by specific generators.  Called only once for each instance to ensure
   * that rule application does not change the output schema of a generator.
   */
  protected def makeOutput(): Seq[Attribute]

  private var _output: Seq[Attribute] = null

  def output: Seq[Attribute] = {
    if (_output == null) {
      _output = makeOutput()
    }
    _output
  }

  /** Should be implemented by child classes to perform specific Generators. */
  def apply(input: Row): TraversableOnce[Row]

  /** Overridden `makeCopy` also copies the attributes that are produced by this generator. */
  override def makeCopy(newArgs: Array[AnyRef]): this.type = {
    val copy = super.makeCopy(newArgs)
    copy._output = _output
    copy
  }
}

/**
 * Given an input array produces a sequence of rows for each value in the array.
 */
case class Explode(attributeName: String, child: Expression)
  extends Generator with trees.UnaryNode[Expression] {

  override lazy val resolved = child.resolved && child.dataType.isInstanceOf[ArrayType]

  lazy val elementType = child.dataType match {
    case ArrayType(et) => et
  }

  protected def makeOutput() =
    AttributeReference(attributeName, elementType, nullable = true)() :: Nil

  def apply(input: Row): TraversableOnce[Row] = {
    val inputArray = Evaluate(child, Vector(input)).asInstanceOf[Seq[Any]]
    if (inputArray == null) Nil else inputArray.map(v => new GenericRow(Vector(v)))
  }

  override def toString() = s"explode($child)"
}

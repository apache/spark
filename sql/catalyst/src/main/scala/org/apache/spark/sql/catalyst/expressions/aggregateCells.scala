package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types._

/**
 * :: DeveloperApi ::
 * Cells used to support aggregating over nested fields.
 * @param child the input data source.
 */
case class SumCell(child: Expression) extends UnaryExpression{
  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    evalE match {
      case seq: Seq[Any] => seq.reduce((a, b) => numeric.plus(a, b))
      case _ => evalE
    }
  }

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  lazy val numeric = dataType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => sys.error(s"Type $other does not support numeric operations")
  }

  override def dataType: DataType = child.dataType match {
    case DecimalType.Fixed(_, _) =>
      DecimalType.Unlimited
    case ArrayType(dataType, _) =>
      dataType
    case _ =>
      child.dataType
  }
}

case class CountCell(child: Expression) extends UnaryExpression{
  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    evalE match {
      case seq: Seq[Any] => seq.size.toLong
      case p if p != null => 1L
      case _ => null
    }
  }

  override def nullable: Boolean = false
  override def dataType: DataType = LongType
}

case class MinCell(child: Expression) extends UnaryExpression{
  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    evalE match {
      case seq: Seq[Any] => seq.reduce((a, b) => if (ordering.compare(a, b) < 0) a else b)
      case _ => evalE
    }
  }

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  lazy val ordering = dataType match {
    case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
    case other => sys.error(s"Type $other does not support ordered operations")
  }

  override def dataType: DataType = child.dataType match {
    case DecimalType.Fixed(_, _) =>
      DecimalType.Unlimited
    case ArrayType(dataType, _) =>
      dataType
    case _ =>
      child.dataType
  }
}

case class MaxCell(child: Expression) extends UnaryExpression{
  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    evalE match {
      case seq: Seq[Any] => seq.reduce((a, b) => if (ordering.compare(a, b) > 0) a else b)
      case _ => evalE
    }
  }

  lazy val ordering = dataType match {
    case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
    case other => sys.error(s"Type $other does not support ordered operations")
  }

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType match {
    case DecimalType.Fixed(_, _) =>
      DecimalType.Unlimited
    case ArrayType(dataType, _) =>
      dataType
    case _ =>
      child.dataType
  }
}
package catalyst
package expressions

abstract class NamedExpression extends Expression {
  self: Product =>
}

abstract class Attribute extends NamedExpression {
  self: Product =>
}
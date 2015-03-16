package breeze.optimize.proximal

/**
 * Supported constraints by QuadraticMinimizer object
 * @author debasish83
 */
object Constraint extends Enumeration {
  type Constraint = Value
  val IDENTITY, SMOOTH, POSITIVE, BOX, SPARSE, EQUALITY, PROBABILITYSIMPLEX = Value
}
package fix

import scalafix.v1._
import scala.meta._

class AccumulatorUpgrade extends SemanticRule("AccumulatorUpgrade") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    val accumulatorFunMatch = SymbolMatcher.normalized("org.apache.spark.SparkContext.accumulator")
    val utils = new Utils()

    def matchOnTree(e: Tree): Patch = {
      e match {
        // non-named accumulator
        case ns @ Term.Apply(accumulatorFunMatch(f), params) =>
          params match {
            case List(param) =>
              param match {
                case utils.intMatcher(initialValue) =>
                  println("Matched initial value " + initialValue)
                  Patch.empty
                case utils.longMatcher(initialValue) =>
                case utils.doubleMatcher(initialValue) =>
                  println("Matched initial value " + initialValue)
                  Patch.empty
                case _ =>
                  println("Failed to match acc param " + param)
                  Patch.empty
              }
            case List(param, name) =>
              println("Failed to match acc param w/name " + param)
              Patch.empty
          }
        case elem @ _ =>
          elem.children match {
            case Nil => Patch.empty
            case _ => elem.children.map(matchOnTree).asPatch
          }
      }
    }
    matchOnTree(doc.tree)
  }
}

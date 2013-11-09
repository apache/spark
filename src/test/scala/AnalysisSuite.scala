package catalyst
package frontend

import org.scalatest.FunSuite

import analysis._
import expressions._
import plans.logical._
import types._

class AnalysisSuite extends FunSuite {
  val analyze = new Analyzer(EmptyCatalog)

  val testRelation = new TestRelation(AttributeReference("a", IntegerType)() :: Nil)

  test("analyze project") {
    assert(analyze(Project(Seq(UnresolvedAttribute("a")), testRelation)) === Project(testRelation.output, testRelation))

  }
}
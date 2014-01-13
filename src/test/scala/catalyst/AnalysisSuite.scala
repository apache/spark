package catalyst
package analysis

import org.scalatest.FunSuite

import analysis._
import expressions._
import plans.logical._
import types._

import dsl._

class AnalysisSuite extends FunSuite {
  val analyze = SimpleAnalyzer

  val testRelation = LocalRelation('a.int)

  test("analyze project") {
    assert(analyze(Project(Seq(UnresolvedAttribute("a")), testRelation)) === Project(testRelation.output, testRelation))

  }
}
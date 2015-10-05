package examples

object patterns {

  trait Tree
  case class Branch(left: Tree, right: Tree) extends Tree
  case class Leaf(x: Int) extends Tree

  val tree1 = Branch(Branch(Leaf(1), Leaf(2)), Branch(Leaf(3), Leaf(4)))

  def sumLeaves(t: Tree): Int = t match {
    case Branch(l, r) => sumLeaves(l) + sumLeaves(r)
    case Leaf(x) => x
  }

  def find[a,b](it: Iterator[Pair[a, b]], x: a): Option[b] = {
    var result: Option[b] = None
    var found = false
    while (it.hasNext && !found) {
      val Pair(x1, y) = it.next
      if (x == x1) { found = true; result = Some(y) }
    }
    result
  }

  def printFinds[a](xs: List[Pair[a, String]], x: a) =
    find(xs.iterator, x) match {
      case Some(y) => println(y)
      case None => println("no match")
    }

  def main(args: Array[String]) {
    println("sum of leafs=" + sumLeaves(tree1))
    printFinds(List(Pair(3, "three"), Pair(4, "four")), 4)
  }
}

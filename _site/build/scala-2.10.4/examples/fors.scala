package examples

import scala.xml._

// May the fors be with you...
object fors {

  class Person(_name: String, _age: Int) {
    val name = _name
    val age = _age
  }

  def printOlderThan20(xs: Seq[Person]): Iterator[String] =
    printOlderThan20(xs.iterator)

  def printOlderThan20(xs: Iterator[Person]): Iterator[String] =
    for (p <- xs if p.age > 20) yield p.name

  val persons = List(
    new Person("John", 40),
    new Person("Richard", 68)
  )

  def divisors(n: Int): List[Int] =
    for (i <- List.range(1, n+1) if n % i == 0) yield i

  def isPrime(n: Int) = divisors(n).length == 2

  def findNums(n: Int): Iterable[(Int, Int)] =
    for {
      i <- 1 until n
      j <- 1 until (i-1)
      if isPrime(i+j)
    } yield (i, j)

  def sum(xs: List[Double]): Double =
    xs.foldLeft(0.0) { (x, y) => x + y }

  def scalProd(xs: List[Double], ys: List[Double]) =
    sum(for((x, y) <- xs zip ys) yield x * y)

  val prefix = null
  val scope = TopScope
  val e = Node.NoAttributes

  def elem(label: String, child: Node*) = Elem(prefix, label, e, scope, true, child: _*)
  val rawBooks = List(
    elem("book",
         elem("title", Text("Structure and Interpretation of Computer Programs")),
         elem("author", Text("Abelson, Harald")),
         elem("author", Text("Sussman, Gerald J."))),
    elem("book",
         elem("title", Text("Principles of Compiler Design")),
         elem("author", Text("Aho, Alfred")),
         elem("author", Text("Ullman, Jeffrey"))),
    elem("book",
         elem("title", Text("Principles of Compiler Design")),
         elem("author", Text("Odersky, Martin")),
         elem("author", Text("Phillips, Paul")),
         elem("author", Text("et al.")),
         elem("notes", Text("backordered")),
         elem("notes", Text("by popular demand"))),
    elem("book",
         elem("title", Text("Structure and Interpretation of Computer Programs")),
         elem("author", Text("Abelson, Harald")),
         elem("author", Text("Sussman, Gerald J."))),
    elem("book",
         elem("title", Text("Programming in Modula-2")),
         elem("author", Text("Wirth, Niklaus")))
  )
  val books = removeDuplicates(rawBooks)

  def searchBooks(books: List[Any], keyword: String, searchTag: String, resultTag: String) =
    for {
      Elem(_, "book", _, _, bookinfo @ _*) <- books
      Elem(_, `searchTag`, _, _, Text(search)) <- bookinfo.toList
      if search contains keyword
      Elem(_, `resultTag`, _, _, Text(result)) <- bookinfo.toList
    } yield result

  def findAuthorOf(what: String) = searchBooks(books, what, "title", "author")
  def findTitleBy(who: String) = searchBooks(books, who, "author", "title")

  // stack-consuming, for comparison
  def removeDuplicatesStackful[A](xs: List[A]): List[A] =
    if (xs.isEmpty) xs
    else xs.head :: removeDuplicatesStackful(for (x <- xs.tail if x != xs.head) yield x)

  // as in List.distinct
  def removeDuplicates[A](xs: List[A]): List[A] = {
    import collection.mutable.{HashSet, ListBuffer}
    val buf = ListBuffer[A]()
    val seen = HashSet[A]()
    for (x <- xs)
      if (!seen(x)) {
        buf += x
        seen += x
      }
    buf.toList
  }

  def main(args: Array[String]) {
    print("Persons over 20:")
    printOlderThan20(persons) foreach { x => print(" " + x) }
    println

    println("divisors(34) = " + divisors(34))

    print("findNums(15) =")
    findNums(15) foreach { x => print(" " + x) }
    println

    val xs = List(3.5, 5.0, 4.5)
    println("average(" + xs + ") = " + sum(xs) / xs.length)

    val ys = List(2.0, 1.0, 3.0)
    println("scalProd(" + xs + ", " + ys +") = " + scalProd(xs, ys))

    val pp = new PrettyPrinter(80, 2)
    def show(ns: Node*) {
      ns foreach (n => print(pp.format(n)))
      println
    }
    show(rawBooks: _*)
    println("unique books:")
    show(books: _*)
    println("Authors of books like 'Program':")
    println(findAuthorOf("Program") mkString "; ")
    println("Authors of books like 'Principles':")
    println(findAuthorOf("Principles") mkString "; ")
    println("Books by 'Ullman':")
    println(findTitleBy("Ullman") mkString "; ")
  }

}

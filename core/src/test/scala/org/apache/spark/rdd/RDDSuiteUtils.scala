package org.apache.spark.rdd

object RDDSuiteUtils {
  case class Person(first: String, last: String, age: Int)

  object AgeOrdering extends Ordering[Person] {
    def compare(a:Person, b:Person) = a.age compare b.age
  }

  object NameOrdering extends Ordering[Person] {
    def compare(a:Person, b:Person) =
      implicitly[Ordering[Tuple2[String,String]]].compare((a.last, a.first), (b.last, b.first))
  }
}

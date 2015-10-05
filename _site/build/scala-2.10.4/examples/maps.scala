package examples

object maps {

  import scala.collection.immutable._

  trait MapStruct[kt, vt] {
    trait Map extends Function1[kt, vt] {
      def extend(key: kt, value: vt): Map
      def remove(key: kt): Map
      def domain: Stream[kt]
      def range: Stream[vt]
    }
    type map <: Map
    val empty: map
  }

  class AlgBinTree[kt >: Null <: Ordered[kt], vt >: Null <: AnyRef]() extends MapStruct[kt, vt] {
    type map = AlgMap

    val empty: AlgMap = Empty()

    private case class Empty() extends AlgMap {}
    private case class Node(key: kt, value: vt, l: map, r: map) extends AlgMap {}

    trait AlgMap extends Map {
      def apply(key: kt): vt = this match {
        case Empty() => null
        case Node(k, v, l, r) =>
          if (key < k) l.apply(key)
          else if (key > k) r.apply(key)
          else v
      }

      def extend(key: kt, value: vt): map = this match {
        case Empty()=> Node(key, value, empty, empty)
        case Node(k, v, l, r) =>
          if (key < k) Node(k, v, l.extend(key, value), r)
          else if (key > k) Node(k, v, l, r.extend(key, value))
          else Node(k, value, l, r)
      }

      def remove(key: kt): map = this match {
        case Empty()=> empty
        case Node(k, v, l, r) =>
          if (key < k) Node(k, v, l.remove(key), r)
          else if (key > k) Node(k, v, l, r.remove(key))
          else if (l == empty) r
          else if (r == empty) l
          else {
          val midKey = r.domain.head
          Node(midKey, r.apply(midKey), l, r.remove(midKey))
        }
      }

      def domain: Stream[kt] = this match {
        case Empty()=> Stream.empty
        case Node(k, v, l, r) => l.domain append Stream.cons(k, r.domain)
      }

      def range: Stream[vt] = this match {
        case Empty()=> Stream.empty
        case Node(k, v, l, r) => l.range append Stream.cons(v, r.range)
      }
    }
  }

  class OOBinTree[kt >: Null <: Ordered[kt], vt >: Null <: AnyRef]() extends MapStruct[kt, vt] {
    type map = OOMap

    trait OOMap extends Map {
      def apply(key: kt): vt
      def extend(key: kt, value: vt): map
      def remove(key: kt): map
      def domain: Stream[kt]
      def range: Stream[vt]
    }
    val empty: OOMap = new OOMap {
      def apply(key: kt): vt = null
      def extend(key: kt, value: vt) = new Node(key, value, empty, empty)
      def remove(key: kt) = empty
      def domain: Stream[kt] = Stream.empty
      def range: Stream[vt] = Stream.empty
    }
    private class Node(k: kt, v: vt, l: map, r: map) extends OOMap {
      def apply(key: kt): vt =
        if (key < k) l.apply(key)
        else if (key > k) r.apply(key)
        else v
      def extend(key: kt, value: vt): map =
        if (key < k) new Node(k, v, l.extend(key, value), r)
        else if (key > k) new Node(k, v, l, r.extend(key, value))
        else new Node(k, value, l, r)
      def remove(key: kt): map =
        if (key < k) new Node(k, v, l.remove(key), r)
        else if (key > k) new Node(k, v, l, r.remove(key))
        else if (l == empty) r
        else if (r == empty) l
        else {
          val midKey = r.domain.head
          new Node(midKey, r(midKey), l, r.remove(midKey))
        }
      def domain: Stream[kt] = l.domain append Stream.cons(k, r.domain)
      def range: Stream[vt] = l.range append Stream.cons(v, r.range)
    }
  }

  class MutBinTree[kt >: Null <: Ordered[kt], vt >: Null <: AnyRef]() extends MapStruct[kt, vt] {
    type map = MutMap
    class MutMap(key: kt, value: vt) extends Map {
      val k = key
      var v = value
      var l, r = empty

      def apply(key: kt): vt =
        if (this == empty) null
        else if (key < k) l.apply(key)
        else if (key > k) r.apply(key)
        else v

      def extend(key: kt, value: vt): map =
        if (this == empty) new MutMap(key, value)
        else {
          if (key < k) l = l.extend(key, value)
          else if (key > k) r = r.extend(key, value)
          else v = value
          this
        }

      def remove(key: kt): map =
        if (this == empty) this
        else if (key < k) { l = l.remove(key); this }
        else if (key > k) { r = r.remove(key); this }
        else if (l == empty) r
        else if (r == empty) l
        else {
          var mid = r
          while (!(mid.l == empty)) { mid = mid.l }
          mid.r = r.remove(mid.k)
          mid.l = l
          mid
        }

      def domain: Stream[kt] =
        if (this == empty) Stream.empty
        else l.domain append Stream.cons(k, r.domain)

      def range: Stream[vt] =
        if (this == empty) Stream.empty
        else l.range append Stream.cons(v, r.range)
    }
    val empty = new MutMap(null, null)
  }

  class Date(y: Int, m: Int, d: Int) extends Ordered[Date] {
    def year = y
    def month = m
    def day = d

    def compare(other: Date): Int =
      if (year == other.year &&
          month == other.month &&
          day == other.day)
        0
      else if (year < other.year ||
               year == other.year && month < other.month ||
               month == other.month && day < other.day)
        -1
      else
        1

    override def equals(that: Any): Boolean =
      that.isInstanceOf[Date] && {
        val o = that.asInstanceOf[Date]
        day == o.day && month == o.month && year == o.year
      }
  }

  def main(args: Array[String]) {
    val t = new OOBinTree[Date, String]()
    ()
  }

}




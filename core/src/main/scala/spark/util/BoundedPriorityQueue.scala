package spark.util

import java.util.{PriorityQueue => JPriorityQueue}
import scala.collection.generic.Growable

/**
 * Bounded priority queue. This class modifies the original PriorityQueue's
 * add/offer methods such that only the top K elements are retained. The top
 * K elements are defined by an implicit Ordering[A].
 */
class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends JPriorityQueue[A](maxSize, ord) with Growable[A] {

  override def offer(a: A): Boolean  = {
    if (size < maxSize) super.offer(a)
    else maybeReplaceLowest(a)
  }

  override def add(a: A): Boolean = offer(a)

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach(add)
    this
  }

  override def +=(elem: A): this.type = {
    add(elem)
    this
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = peek()
    if (head != null && ord.gt(a, head)) {
      poll()
      super.offer(a)
    } else false
  }
}

object BoundedPriorityQueue {
  import scala.collection.JavaConverters._
  implicit def asIterable[A](queue: BoundedPriorityQueue[A]): Iterable[A] = queue.asScala
}


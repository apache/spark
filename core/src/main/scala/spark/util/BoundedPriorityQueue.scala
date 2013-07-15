package spark.util

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}
import scala.collection.generic.Growable
import scala.collection.JavaConverters._

/**
 * Bounded priority queue. This class wraps the original PriorityQueue
 * class and modifies it such that only the top K elements are retained.
 * The top K elements are defined by an implicit Ordering[A].
 */
class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  override def +=(elem: A): this.type = {
    if (size < maxSize) underlying.offer(elem)
    else maybeReplaceLowest(elem)
    this
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear() { underlying.clear() }

  private def maybeReplaceLowest(a: A): Boolean = {
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else false
  }
}


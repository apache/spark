// This is a copy of Scala 2.7.7's Range class, (c) 2006-2009, LAMP/EPFL.
// The only change here is to make it Serializable, because Ranges aren't.
// This won't be needed in Scala 2.8, where Scala's Range becomes Serializable.

package spark

@serializable
private class SerializableRange(val start: Int, val end: Int, val step: Int)
extends RandomAccessSeq.Projection[Int] {
  if (step == 0) throw new Predef.IllegalArgumentException

  /** Create a new range with the start and end values of this range and
   *  a new <code>step</code>.
   */
  def by(step: Int): Range = new Range(start, end, step)

  override def foreach(f: Int => Unit) {
    if (step > 0) {
      var i = this.start
      val until = if (inInterval(end)) end + 1 else end

      while (i < until) {
        f(i)
        i += step
      }
    } else {
      var i = this.start
      val until = if (inInterval(end)) end - 1 else end

      while (i > until) {
        f(i)
        i += step
      }
    }
  }

  lazy val length: Int = {
    if (start < end && this.step < 0) 0
    else if (start > end && this.step > 0) 0
    else {
      val base = if (start < end) end - start
                 else start - end
      assert(base >= 0)
      val step = if (this.step < 0) -this.step else this.step
      assert(step >= 0)
      base / step + last(base, step)
    }
  }

  protected def last(base: Int, step: Int): Int =
    if (base % step != 0) 1 else 0

  def apply(idx: Int): Int = {
    if (idx < 0 || idx >= length) throw new Predef.IndexOutOfBoundsException
    start + (step * idx)
  }

  /** a <code>Seq.contains</code>, not a <code>Iterator.contains</code>! */
  def contains(x: Int): Boolean = {
    inInterval(x) && (((x - start) % step) == 0)
  }

  /** Is the argument inside the interval defined by `start' and `end'? 
   *  Returns true if `x' is inside [start, end).
   */
  protected def inInterval(x: Int): Boolean =
    if (step > 0) 
      (x >= start && x < end) 
    else 
      (x <= start && x > end)

  //def inclusive = new Range.Inclusive(start,end,step)

  override def toString = "SerializableRange(%d, %d, %d)".format(start, end, step)
}

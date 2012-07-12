package spark

import java.io._

import scala.collection.mutable.Map

class Accumulator[T] (
    @transient initialValue: T,
    param: AccumulatorParam[T])
  extends Serializable {
  
  val id = Accumulators.newId
  @transient
  var value_ = initialValue // Current value on master
  val zero = param.zero(initialValue)  // Zero value to be passed to workers
  var deserialized = false

  Accumulators.register(this, true)

  def += (term: T) { value_ = param.addInPlace(value_, term) }
  def value = this.value_
  def value_= (t: T) {
    if (!deserialized) value_ = t
    else throw new UnsupportedOperationException("Can't use value_= in task")
  }
 
  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    value_ = zero
    deserialized = true
    Accumulators.register(this, false)
  }

  override def toString = value_.toString
}

class Accumulatable[T,Y](
    @transient initialValue: T,
    param: AccumulatableParam[T,Y]) extends Accumulator[T](initialValue, param) {
  /**
   * add more data to the current value of the this accumulator, via
   * AccumulatableParam.addToAccum
   * @param term added to the current value of the accumulator
   */
  def +:= (term: Y) {value_ = param.addToAccum(value_, term)}
}

/**
 * A datatype that can be accumulated, ie. has a commutative & associative +
 * @tparam T
 */
trait AccumulatorParam[T] extends Serializable {
  def addInPlace(t1: T, t2: T): T
  def zero(initialValue: T): T
}

/**
 * A datatype that can be accumulated.  Slightly extends [[spark.AccumulatorParam]] to allow you to
 * combine a different data type with value so far
 * @tparam T the full accumulated data
 * @tparam Y partial data that can be added in
 */
trait AccumulatableParam[T,Y] extends AccumulatorParam[T] {
  /**
   * Add additional data to the accumulator value.
   * @param t1 the current value of the accumulator
   * @param t2 the data to be added to the accumulator
   * @return the new value of the accumulator
   */
  def addToAccum(t1: T, t2: Y) : T
}

// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private object Accumulators {
  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, Accumulator[_]]()
  val localAccums = Map[Thread, Map[Long, Accumulator[_]]]()
  var lastId: Long = 0
  
  def newId: Long = synchronized {
    lastId += 1
    return lastId
  }

  def register(a: Accumulator[_], original: Boolean) {
    synchronized {
      if (original) {
        originals(a.id) = a
      } else {
        val accums = localAccums.getOrElseUpdate(Thread.currentThread, Map())
        accums(a.id) = a
      }
    }
  }

  // Clear the local (non-original) accumulators for the current thread
  def clear() {
    synchronized {
      localAccums.remove(Thread.currentThread)
    }
  }

  // Get the values of the local accumulators for the current thread (by ID)
  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for ((id, accum) <- localAccums.getOrElse(Thread.currentThread, Map())) {
      ret(id) = accum.value
    }
    return ret
  }

  // Add values to the original accumulators with some given IDs
  def add(values: Map[Long, Any]) {
    synchronized {
      for ((id, value) <- values) {
        if (originals.contains(id)) {
          originals(id).asInstanceOf[Accumulator[Any]] += value
        }
      }
    }
  }
}

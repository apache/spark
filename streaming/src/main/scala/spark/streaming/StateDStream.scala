package spark.streaming

import spark.RDD
import spark.Partitioner
import spark.MapPartitionsRDD
import spark.SparkContext._


class StateDStream[K: ClassManifest, V: ClassManifest, S <: AnyRef : ClassManifest](
    parent: DStream[(K, V)],
    updateFunc: (Iterator[(K, Seq[V], S)]) => Iterator[(K, S)],
    partitioner: Partitioner,
    rememberPartitioner: Boolean
  ) extends DStream[(K, S)](parent.ssc) {

  class SpecialMapPartitionsRDD[U: ClassManifest, T: ClassManifest](prev: RDD[T], f: Iterator[T] => Iterator[U])
    extends MapPartitionsRDD(prev, f) {
    override val partitioner = if (rememberPartitioner) prev.partitioner else None
  }

  override def dependencies = List(parent)

  override def slideTime = parent.slideTime

  override def compute(validTime: Time): Option[RDD[(K, S)]] = {

    // Try to get the previous state RDD
    getOrCompute(validTime - slideTime) match {

      case Some(prevStateRDD) => {    // If previous state RDD exists

        // Define the function for the mapPartition operation on cogrouped RDD;
        // first map the cogrouped tuple to tuples of required type,
        // and then apply the update function
        val func = (iterator: Iterator[(K, (Seq[V], Seq[S]))]) => {
          val i = iterator.map(t => {
            (t._1, t._2._1, t._2._2.headOption.getOrElse(null.asInstanceOf[S]))
          })
          updateFunc(i)
        }

        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) => {   // If parent RDD exists, then compute as usual
            val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
            val stateRDD = new SpecialMapPartitionsRDD(cogroupedRDD, func)
            logDebug("Generating state RDD for time " + validTime)
            return Some(stateRDD)
          }
          case None => {    // If parent RDD does not exist, then return old state RDD
            logDebug("Generating state RDD for time " + validTime + " (no change)")
            return Some(prevStateRDD)
          }
        }
      }

      case None => {    // If previous session RDD does not exist (first input data)

        // Define the function for the mapPartition operation on grouped RDD;
        // first map the grouped tuple to tuples of required type,
        // and then apply the update function
        val func = (iterator: Iterator[(K, Seq[V])]) => {
          updateFunc(iterator.map(tuple => (tuple._1, tuple._2, null.asInstanceOf[S])))
        }

        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) => {   // If parent RDD exists, then compute as usual
            val groupedRDD = parentRDD.groupByKey(partitioner)
            val sessionRDD = new SpecialMapPartitionsRDD(groupedRDD, func)
            logDebug("Generating state RDD for time " + validTime + " (first)")
            return Some(sessionRDD)
          }
          case None => { // If parent RDD does not exist, then nothing to do!
            logDebug("Not generating state RDD (no previous state, no parent)")
            return None
          }
        }
      }
    }
  }
}

package spark.streaming

import spark.RDD
import spark.rdd.BlockRDD
import spark.Partitioner
import spark.rdd.MapPartitionsRDD
import spark.SparkContext._
import spark.storage.StorageLevel


class StateRDD[U: ClassManifest, T: ClassManifest](
    prev: RDD[T],
    f: Iterator[T] => Iterator[U],
    rememberPartitioner: Boolean
  ) extends MapPartitionsRDD[U, T](prev, f) {
  override val partitioner = if (rememberPartitioner) prev.partitioner else None
}

class StateDStream[K: ClassManifest, V: ClassManifest, S <: AnyRef : ClassManifest](
    parent: DStream[(K, V)],
    updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
    partitioner: Partitioner,
    rememberPartitioner: Boolean
  ) extends DStream[(K, S)](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY)

  override def dependencies = List(parent)

  override def slideTime = parent.slideTime

  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[(K, S)]] = {

    // Try to get the previous state RDD
    getOrCompute(validTime - slideTime) match {

      case Some(prevStateRDD) => {    // If previous state RDD exists

        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) => {   // If parent RDD exists, then compute as usual

            // Define the function for the mapPartition operation on cogrouped RDD;
            // first map the cogrouped tuple to tuples of required type,
            // and then apply the update function
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, (Seq[V], Seq[S]))]) => {
              val i = iterator.map(t => {
                (t._1, t._2._1, t._2._2.headOption)
              })
              updateFuncLocal(i)
            }
            val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
            val stateRDD = new StateRDD(cogroupedRDD, finalFunc, rememberPartitioner)
            //logDebug("Generating state RDD for time " + validTime)
            return Some(stateRDD)
          }
          case None => {    // If parent RDD does not exist, then return old state RDD
            return Some(prevStateRDD)
          }
        }
      }

      case None => {    // If previous session RDD does not exist (first input data)

        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) => {   // If parent RDD exists, then compute as usual

            // Define the function for the mapPartition operation on grouped RDD;
            // first map the grouped tuple to tuples of required type,
            // and then apply the update function
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, Seq[V])]) => {
              updateFuncLocal(iterator.map(tuple => (tuple._1, tuple._2, None)))
            }

            val groupedRDD = parentRDD.groupByKey(partitioner)
            val sessionRDD = new StateRDD(groupedRDD, finalFunc, rememberPartitioner)
            //logDebug("Generating state RDD for time " + validTime + " (first)")
            return Some(sessionRDD)
          }
          case None => { // If parent RDD does not exist, then nothing to do!
            //logDebug("Not generating state RDD (no previous state, no parent)")
            return None
          }
        }
      }
    }
  }
}

package spark.streaming.dstream

import spark.RDD
import spark.Partitioner
import spark.SparkContext._
import spark.storage.StorageLevel
import spark.streaming.{Duration, Time, DStream}

private[streaming]
class StateDStream[K: ClassManifest, V: ClassManifest, S: ClassManifest](
    parent: DStream[(K, V)],
    updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
    partitioner: Partitioner,
    preservePartitioning: Boolean
  ) extends DStream[(K, S)](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY_SER)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[(K, S)]] = {

    // Try to get the previous state RDD
    getOrCompute(validTime - slideDuration) match {

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
            val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
            //logDebug("Generating state RDD for time " + validTime)
            return Some(stateRDD)
          }
          case None => {    // If parent RDD does not exist

            // Re-apply the update function to the old state RDD
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, S)]) => {
              val i = iterator.map(t => (t._1, Seq[V](), Option(t._2)))
              updateFuncLocal(i)
            }
            val stateRDD = prevStateRDD.mapPartitions(finalFunc, preservePartitioning)
            return Some(stateRDD)
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
            val sessionRDD = groupedRDD.mapPartitions(finalFunc, preservePartitioning)
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

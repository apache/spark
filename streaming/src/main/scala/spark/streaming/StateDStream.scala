package spark.streaming

import spark.RDD
import spark.BlockRDD
import spark.Partitioner
import spark.MapPartitionsRDD
import spark.SparkContext._
import spark.storage.StorageLevel

class StateDStream[K: ClassManifest, V: ClassManifest, S <: AnyRef : ClassManifest](
    @transient parent: DStream[(K, V)],
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

  override def getOrCompute(time: Time): Option[RDD[(K, S)]] = {
    generatedRDDs.get(time) match {
      case Some(oldRDD) => {
        if (checkpointInterval != null && time > zeroTime && (time - zeroTime).isMultipleOf(checkpointInterval) && oldRDD.dependencies.size > 0) {
          val r = oldRDD
          val oldRDDBlockIds = oldRDD.splits.map(s => "rdd:" + r.id + ":" + s.index)
          val checkpointedRDD = new BlockRDD[(K, S)](ssc.sc, oldRDDBlockIds) {
            override val partitioner = oldRDD.partitioner
          }
          generatedRDDs.update(time, checkpointedRDD)
          logInfo("Checkpointed RDD " + oldRDD.id + " of time " + time + " with its new RDD " + checkpointedRDD.id)
          Some(checkpointedRDD)
        } else {
          Some(oldRDD)
        }
      }
      case None => {
        if (isTimeValid(time)) {
          compute(time) match {
            case Some(newRDD) => {
              if (checkpointInterval != null && (time - zeroTime).isMultipleOf(checkpointInterval)) { 
                newRDD.persist(checkpointLevel)
                logInfo("Persisting " + newRDD + " to " + checkpointLevel + " at time " + time)
              } else if (storageLevel != StorageLevel.NONE) {
                newRDD.persist(storageLevel)
                logInfo("Persisting " + newRDD + " to " + storageLevel + " at time " + time)
              }
              generatedRDDs.put(time, newRDD)
              Some(newRDD)
            }
            case None => {
              None
            }
          }
        } else {
          None
        }
      }
    }
  }
  
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
            val mapPartitionFunc = (iterator: Iterator[(K, (Seq[V], Seq[S]))]) => {
              val i = iterator.map(t => {
                (t._1, t._2._1, t._2._2.headOption.getOrElse(null.asInstanceOf[S]))
              })
              updateFuncLocal(i)
            }
            val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
            val stateRDD = new SpecialMapPartitionsRDD(cogroupedRDD, mapPartitionFunc)
            //logDebug("Generating state RDD for time " + validTime)
            return Some(stateRDD)
          }
          case None => {    // If parent RDD does not exist, then return old state RDD
            //logDebug("Generating state RDD for time " + validTime + " (no change)")
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
            val mapPartitionFunc = (iterator: Iterator[(K, Seq[V])]) => {
                updateFuncLocal(iterator.map(tuple => (tuple._1, tuple._2, null.asInstanceOf[S])))
            }

            val groupedRDD = parentRDD.groupByKey(partitioner)
            val sessionRDD = new SpecialMapPartitionsRDD(groupedRDD, mapPartitionFunc)
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

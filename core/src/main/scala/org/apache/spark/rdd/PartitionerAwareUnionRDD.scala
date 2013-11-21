package org.apache.spark.rdd

import org.apache.spark.{TaskContext, OneToOneDependency, SparkContext, Partition}

private[spark]
class PartitionerAwareUnionRDDPartition(val idx: Int, val partitions: Array[Partition])
  extends Partition {
  override val index = idx
  override def hashCode(): Int = idx
}

private[spark]
class PartitionerAwareUnionRDD[T: ClassManifest](
    sc: SparkContext,
    var rdds: Seq[RDD[T]]
  ) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.length > 0)
  require(rdds.flatMap(_.partitioner).toSet.size == 1,
    "Parent RDDs have different partitioners: " + rdds.flatMap(_.partitioner))

  override val partitioner = rdds.head.partitioner

  override def getPartitions: Array[Partition] = {
    val numPartitions = rdds.head.partitions.length
    (0 until numPartitions).map(index => {
      val parentPartitions = rdds.map(_.partitions(index)).toArray
      new PartitionerAwareUnionRDDPartition(index, parentPartitions)
    }).toArray
  }

  // Get the location where most of the partitions of parent RDDs are located
  override def getPreferredLocations(s: Partition): Seq[String] = {
    logDebug("Getting preferred locations for " + this)
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].partitions
    val locations = rdds.zip(parentPartitions).flatMap {
      case (rdd, part) => {
        val parentLocations = currPrefLocs(rdd, part)
        logDebug("Location of " + rdd + " partition " + part.index + " = " + parentLocations)
        parentLocations
      }
    }

    if (locations.isEmpty) {
      Seq.empty
    } else  {
      Seq(locations.groupBy(x => x).map(x => (x._1, x._2.length)).maxBy(_._2)._1)
    }
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].partitions
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }

  // gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  private def currPrefLocs(rdd: RDD[_], part: Partition): Seq[String] = {
    rdd.context.getPreferredLocs(rdd, part.index).map(tl => tl.host)
  }
}





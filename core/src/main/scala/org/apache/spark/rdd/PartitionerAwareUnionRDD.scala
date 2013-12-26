package org.apache.spark.rdd

import org.apache.spark.{TaskContext, OneToOneDependency, SparkContext, Partition}
import scala.reflect.ClassTag
import java.io.{ObjectOutputStream, IOException}

private[spark]
class PartitionerAwareUnionRDDPartition(
    @transient val rdds: Seq[RDD[_]],
    val idx: Int
  ) extends Partition {
  var parents = rdds.map(_.partitions(idx)).toArray
  
  override val index = idx
  override def hashCode(): Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent partition at the time of task serialization
    parents = rdds.map(_.partitions(index)).toArray
    oos.defaultWriteObject()
  }
}

private[spark]
class PartitionerAwareUnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]]
  ) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.length > 0)
  require(rdds.flatMap(_.partitioner).toSet.size == 1,
    "Parent RDDs have different partitioners: " + rdds.flatMap(_.partitioner))

  override val partitioner = rdds.head.partitioner

  override def getPartitions: Array[Partition] = {
    val numPartitions = partitioner.get.numPartitions
    (0 until numPartitions).map(index => {
      new PartitionerAwareUnionRDDPartition(rdds, index)
    }).toArray
  }

  // Get the location where most of the partitions of parent RDDs are located
  override def getPreferredLocations(s: Partition): Seq[String] = {
    logDebug("Finding preferred location for " + this + ", partition " + s.index)
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents
    val locations = rdds.zip(parentPartitions).flatMap {
      case (rdd, part) => {
        val parentLocations = currPrefLocs(rdd, part)
        logDebug("Location of " + rdd + " partition " + part.index + " = " + parentLocations)
        parentLocations
      }
    }
    val location = if (locations.isEmpty) {
      None
    } else  {
      // Find the location where maximum number of parent partitions prefer 
      Some(locations.groupBy(x => x).maxBy(_._2.length)._1)
    }
    logDebug("Selected location for " + this + ", partition " + s.index + " = " + location)
    location.toSeq
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }

  // gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  private def currPrefLocs(rdd: RDD[_], part: Partition): Seq[String] = {
    rdd.context.getPreferredLocs(rdd, part.index).map(tl => tl.host)
  }
}

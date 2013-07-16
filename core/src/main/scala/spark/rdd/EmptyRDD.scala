package spark.rdd

import spark.{RDD, SparkContext, SparkEnv, Partition, TaskContext}
import scala.reflect.ClassTag

/**
 * An RDD that is empty, i.e. has no element in it.
 */
class EmptyRDD[T: ClassTag](sc: SparkContext) extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = Array.empty

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    throw new UnsupportedOperationException("empty RDD")
  }
}

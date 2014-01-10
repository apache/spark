package org.apache.spark.graphx

import scala.reflect.ClassTag

package object algorithms {
  implicit def graphToAlgorithms[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED]): Algorithms[VD, ED] = new Algorithms(graph)
}

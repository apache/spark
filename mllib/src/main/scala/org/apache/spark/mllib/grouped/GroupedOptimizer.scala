package org.apache.spark.mllib.grouped

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

/**
 * Created by kellrott on 6/29/14.
 */
trait GroupedOptimizer extends Serializable {

    /**
     * Solve the provided convex optimization problem.
     */
    def optimize(data: RDD[(Int, (Double, Vector))], initialWeights: Map[Int,Vector]): Map[Int,Vector]

}

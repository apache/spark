/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.linalg.distributed


private[mllib] class StripePartitioner(
    val units: Int,
    val unitsPerPart: Int ) extends Partitioner {

    require(units > 0)
    require(unitsPerPart > 0)
	
    override val numPartitions = math.ceil(units * 1.0 / unitsPerPart).toInt
	
    override def getPartition(key: Any): Int = {
    }

    private def getPartitionId(i: Int): Int = {
    }

    override def equals(obj: Any): Boolean = {
    }
}

private[mllib] object StripePartitioner {

}

private[mllib] class StripeMatrix(
    ) extends Matrix {
}

class BlockSparseMatrix(
    val rowBlocks: RDD[(Int, StripeMatrix)],
    val rowLookup: RDD[(Int, StripeMatrix)],
    val rowsPerBlock: Int,
    val colBlocks: RDD[(Int, StripeMatrix)],
    val colLookup: RDD[(Int, StripeMatrix)]
    val colsPerBlock: Int) extends DistributedMatrix with Logging {

  def this(
      val rowBlocks: RDD[(Int, StripeMatrix)],
      val rowsPerBlock: Int,
      val colBlocks: RDD[(Int, StripeMatrix)],
      val colsPerBlock: Int) = { 
	}
	
}

object BlockSparseMatrix {
}

	

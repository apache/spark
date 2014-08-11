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

package org.apache.spark.graphx.impl

import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}

import org.apache.spark.Partitioner
import org.apache.spark.graphx.{PartitionID, VertexId}
import org.apache.spark.rdd.{ShuffledRDD, RDD}


private[graphx]
class VertexRDDFunctions[VD: ClassTag](self: RDD[(VertexId, VD)]) {
  def copartitionWithVertices(partitioner: Partitioner): RDD[(VertexId, VD)] = {
    val rdd = new ShuffledRDD[VertexId, VD, VD](self, partitioner)

    // Set a custom serializer if the data is of int or double type.
    if (classTag[VD] == ClassTag.Int) {
      rdd.setSerializer(new IntAggMsgSerializer)
    } else if (classTag[VD] == ClassTag.Long) {
      rdd.setSerializer(new LongAggMsgSerializer)
    } else if (classTag[VD] == ClassTag.Double) {
      rdd.setSerializer(new DoubleAggMsgSerializer)
    }
    rdd
  }
}

private[graphx]
object VertexRDDFunctions {
  implicit def rdd2VertexRDDFunctions[VD: ClassTag](rdd: RDD[(VertexId, VD)]) = {
    new VertexRDDFunctions(rdd)
  }
}

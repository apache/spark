package org.apache.spark.graphx

import org.apache.spark.util.collection.OpenHashSet

package object impl {
  private[graphx] type VertexIdToIndexMap = OpenHashSet[VertexID]
}

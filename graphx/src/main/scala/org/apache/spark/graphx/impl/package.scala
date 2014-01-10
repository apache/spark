package org.apache.spark.graphx

import org.apache.spark.util.collection.OpenHashSet

package object impl {
  type VertexIdToIndexMap = OpenHashSet[VertexID]
}

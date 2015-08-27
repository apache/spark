package org.apache.spark

/**
 * Created by hustnn on 8/27/2015.
 */

import org.apache.spark.annotation.DeveloperApi

@DeveloperApi
class MapOutputStatistics(val shuffleId: Int, val bytesByPartitionId: Array[Long])

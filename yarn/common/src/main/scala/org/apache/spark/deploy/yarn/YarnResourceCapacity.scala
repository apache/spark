package org.apache.spark.deploy.yarn


case class YarnAppResource(memory: Int, virtualCores: Int)

case class YarnResourceCapacity(resource: YarnAppResource,
                                ammMemOverHead: Int,
                                executorMemOverhead: Int)


package org.apache.spark.deploy.yarn

case class YarnResourceUsage(numUsedContainers     : Int,
                             numReservedContainers : Int,
                             usedResource     : YarnAppResource,
                             reservedResource : YarnAppResource,
                             neededResource   : YarnAppResource)

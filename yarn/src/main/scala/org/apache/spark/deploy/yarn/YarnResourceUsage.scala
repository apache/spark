package org.apache.spark.deploy.yarn

import org.apache.spark.deploy.yarn.YarnAppResource


case class YarnResourceUsage(numUsedContainers     : Int,
                             numReservedContainers : Int,
                             usedResource     : YarnAppResource,
                             reservedResource : YarnAppResource,
                             neededResource   : YarnAppResource) {

}

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
package org.apache.spark.streaming.scheduler.batch
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging

//noinspection ScalaStyle
class DefaultBatchEstimator(
  proportional: Double, // Decreasing factor
  converge: Double,     // Convergence factor
  gradient: Double,      // Slope of stability line
  stateThreshold: Double // State judgment threshold
) extends BatchEstimator with Logging{

  private var firstRun: Boolean = true      //mark the first run
  private var latestSecondProcessingTime: Long = -1L
  private var latestSecondBatchInterval: Long = -1L
  private var PLarge: Long = -1L      //Larger Processing Time
  private var PSmall: Long = -1L      //Smaller Processing Time
  private var BLarge: Long = -1L      //Larger Batch Interval
  private var BSmall: Long = -1L      //Smaller Batch Interval
  private var currentSlope: Double = -1L
  private var slopeDeviation: Double = -1L
  private var angleDeviation: Double = -1L
  private var nextBatchInterval: Long = -1L
  private var tmpGrd: Double = -1L      // temporary gradient

  var count: AtomicInteger = _          //counter
  var latestStable:Boolean = true
  var latencyCount: AtomicInteger = _

  init()

  def init(){
    logInfo(s"the computer argument proportional: $proportional converge: $converge gradient: $gradient ")
    count = new AtomicInteger(0)
    latencyCount = new AtomicInteger(0)
  }

  /**
    * algorithm to get the next batch interval
    *
    * @param latestProcessingTime
    * @param latestBatchInterval
    *
    * */
  def compute(latestProcessingTime: Long, latestBatchInterval: Long): Option[Long] ={
    logInfo(s"Compute the $count batch interval by latestSecondProcessingTime $latestSecondProcessingTime latestSecondBatchInterval" +
      s" $latestSecondBatchInterval latestProcessingTime $latestProcessingTime "+
      s"and latestBatchInterval $latestBatchInterval  tmpGrd $tmpGrd")

    BLarge = Math.max(latestSecondBatchInterval,latestBatchInterval)
    BSmall = Math.min(latestSecondBatchInterval,latestBatchInterval)

    PLarge = Math.max(latestSecondProcessingTime,latestProcessingTime)
    PSmall = Math.min(latestSecondProcessingTime,latestProcessingTime)

    currentSlope = latestProcessingTime.toDouble / latestBatchInterval.toDouble
    slopeDeviation = Math.atan(gradient) - Math.atan(currentSlope)   // Computing the Slope Difference
    angleDeviation = slopeDeviation * 180 / Math.PI
    logInfo(s"latestStable $latestStable | currentSlope $currentSlope | latencyCount $latencyCount | slopeDeviation $slopeDeviation | angleDeviation $angleDeviation stateThreshold $stateThreshold")

    /** Update the latest processing time and the latest batch interval*/
    def end {
      latestSecondProcessingTime = latestProcessingTime
      latestSecondBatchInterval = latestBatchInterval
    }

    this.synchronized{
      count.set(count.incrementAndGet())

      // enable the function, but not return the result
      if(latestProcessingTime < 200){
        return Some(latestBatchInterval)
      }

      //The first time
      if(firstRun){
        end
        firstRun = false
        return Some(latestBatchInterval)      // unchange batch interval
      }
      // system is convergence
      if(currentSlope < gradient && angleDeviation < converge){
        nextBatchInterval = latestBatchInterval
        end
        latestStable = true
        return Some(nextBatchInterval)
      }else if(currentSlope < gradient && angleDeviation > converge){     // reduce the batch interval
        nextBatchInterval = (Math.floor(proportional * BSmall / 200) * 200).toLong
        latestStable = true
        end
        return Some(nextBatchInterval)
      }else{     // System is not stable
        // must have two unstable situation
        if(latestStable == false) {
          // Batch Interval is not changed
          if (BLarge == BSmall) {
            latencyCount.set(latencyCount.incrementAndGet())
            if(latencyCount.get() >= 2 ){
              tmpGrd = currentSlope
            }else {
              tmpGrd = (latestProcessingTime + latestSecondProcessingTime).toDouble / (latestSecondBatchInterval + latestBatchInterval).toDouble
            }
          } else {
            latencyCount.set(0)   // reset
            tmpGrd = (PLarge - PSmall).toDouble / (BLarge - BSmall).toDouble
          }

          // judge the state
          if (tmpGrd > stateThreshold) {
            // Reduce batch interval
            nextBatchInterval = (Math.floor(proportional * BSmall / 200) * 200).toLong
            end
            return Some(nextBatchInterval)
          } else {
            // Increase batch interval
            nextBatchInterval = (Math.floor(BLarge / gradient / 200) * 200).toLong
            end
            return Some(nextBatchInterval)
          }
        }else{
          latestStable = false
          end
          return Some(latestBatchInterval)
        }
      }
    }
  }
}

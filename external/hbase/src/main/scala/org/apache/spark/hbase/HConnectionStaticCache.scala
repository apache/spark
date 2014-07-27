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

package org.apache.spark.hbase

import java.util.HashMap
import org.apache.hadoop.hbase.client.HConnection
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.Timer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HConnectionManager
import java.util.TimerTask
import scala.collection.mutable.MutableList
import org.apache.spark.Logging

/**
 * A static caching class that will manage all HConnection in a worker
 * 
 * The main idea is there is a hashMap with 
 * HConstants.HBASE_CLIENT_INSTANCE_ID which is ("hbase.client.instance.id")
 * 
 * In that HashMap there is three things
 *   - HConnection
 *   - Number of checked out users of the HConnection
 *   - Time since the HConnection was last used
 *   
 * There is also a Timer thread that will start up every 2 minutes
 * When the Timer thread starts up it will look for HConnection with no
 * checked out users and a last used time that is older then 1 minute.
 * 
 * This class is not intended to be used by Users
 */
object HConnectionStaticCache extends Logging{
  @transient private val hconnectionMap = 
    new HashMap[String, (HConnection, AtomicInteger, AtomicLong)]

  @transient private val hconnectionTimeout = 60000

  @transient private val hconnectionCleaner = new Timer

  hconnectionCleaner.schedule(new hconnectionCleanerTask, hconnectionTimeout * 2)

  /**
   * Gets or starts a HConnection based on a config object
   */
  def getHConnection(config: Configuration): HConnection = {
    val instanceId = config.get(HConstants.HBASE_CLIENT_INSTANCE_ID)
    var hconnectionAndCounter = hconnectionMap.get(instanceId)
    if (hconnectionAndCounter == null) {
      hconnectionMap.synchronized {
        hconnectionAndCounter = hconnectionMap.get(instanceId)
        if (hconnectionAndCounter == null) {
          
          val hConnection = HConnectionManager.createConnection(config)
          hconnectionAndCounter = (hConnection, new AtomicInteger, new AtomicLong)
          hconnectionMap.put(instanceId, hconnectionAndCounter)
          
        }
      }
      logDebug("Created hConnection '" + instanceId +"'");
    } else {
      logDebug("Get hConnection from cache '" + instanceId +"'");
    }
    
    hconnectionAndCounter._2.incrementAndGet()
    return hconnectionAndCounter._1
  }

  /**
   * tell us a thread is no longer using a HConnection
   */
  def finishWithHConnection(config: Configuration, hconnection: HConnection) {
    val instanceId = config.get(HConstants.HBASE_CLIENT_INSTANCE_ID)
    
    var hconnectionAndCounter = hconnectionMap.get(instanceId)
    val usesLeft = hconnectionAndCounter._2.decrementAndGet()
    if (usesLeft == 0) {
      hconnectionAndCounter._3.set(System.currentTimeMillis())
      logDebug("Finished last use of hconnection '" + instanceId +"'");
    } else {
      logDebug("Finished a use of hconnection '" + instanceId +"' with " + usesLeft + " uses left");
    }
    
  }

  /**
   * The timer thread that cleans up the HashMap of Collections
   */
  protected class hconnectionCleanerTask extends TimerTask {
    override def run() {
      
      logDebug("Running hconnectionCleanerTask:" + hconnectionMap.entrySet().size());
      
      val it = hconnectionMap.entrySet().iterator()

      val removeList = new MutableList[String]
      
      while (it.hasNext()) {
        val entry = it.next()
        if (entry.getValue()._2.get() == 0 && 
            entry.getValue()._3.get() + 60000 < System.currentTimeMillis()) {
          removeList.+=(entry.getKey())
        }
      }
      
      if (removeList.length > 0) {
        hconnectionMap.synchronized {
          removeList.foreach(key => {
            val v = hconnectionMap.get(key)
            if (v._2.get() == 0 && 
                v._3.get() + 60000 < System.currentTimeMillis()) {
              
              logDebug("closing hconnection: " + key);
              
              v._1.close()
              
              hconnectionMap.remove(key);
            }   
          }) 
        }
      }
    }
  }

}
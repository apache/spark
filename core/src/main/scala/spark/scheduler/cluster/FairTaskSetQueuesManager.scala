package spark.scheduler.cluster

import java.io.{File, FileInputStream, FileOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks._
import scala.xml._

import spark.Logging
import spark.scheduler.cluster.SchedulingMode.SchedulingMode

/**
 * A Fair Implementation of the TaskSetQueuesManager
 * 
 * Currently we support minShare,weight for fair scheduler between pools
 * Within a pool, it supports FIFO or FS
 * Also, currently we could allocate pools dynamically
 * 
 */
private[spark] class FairTaskSetQueuesManager extends TaskSetQueuesManager with Logging {
  
  val schedulerAllocFile = System.getProperty("spark.fairscheduler.allocation.file","unspecified")  
  val poolNameToPool= new HashMap[String, Pool]
  var pools = new ArrayBuffer[Pool]
  val poolScheduleAlgorithm = new FairSchedulingAlgorithm()
  val POOL_FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.cluster.fair.pool"
  val POOL_DEFAULT_POOL_NAME = "default"
  val POOL_MINIMUM_SHARES_PROPERTY = "minShares"
  val POOL_SCHEDULING_MODE_PROPERTY = "schedulingMode"
  val POOL_WEIGHT_PROPERTY = "weight"
  val POOL_POOL_NAME_PROPERTY = "@name"
  val POOL_POOLS_PROPERTY = "pool"
  val POOL_DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  val POOL_DEFAULT_MINIMUM_SHARES = 2
  val POOL_DEFAULT_WEIGHT = 1
  
  loadPoolProperties()
  
  def loadPoolProperties() {
    //first check if the file exists
    val file = new File(schedulerAllocFile)
    if(file.exists())
    {
      val xml = XML.loadFile(file)
      for (poolNode <- (xml \\ POOL_POOLS_PROPERTY)) {

        val poolName = (poolNode \ POOL_POOL_NAME_PROPERTY).text
        var schedulingMode = POOL_DEFAULT_SCHEDULING_MODE
        var minShares = POOL_DEFAULT_MINIMUM_SHARES
        var weight = POOL_DEFAULT_WEIGHT
        
        
        val xmlSchedulingMode = (poolNode \ POOL_SCHEDULING_MODE_PROPERTY).text
        if( xmlSchedulingMode != "")
        {
          try 
          {
              schedulingMode = SchedulingMode.withName(xmlSchedulingMode)
          }
          catch{
            case e:Exception => logInfo("Error xml schedulingMode, using default schedulingMode") 
          }
        }
        
        val xmlMinShares = (poolNode \ POOL_MINIMUM_SHARES_PROPERTY).text
        if(xmlMinShares != "")
        {
          minShares = xmlMinShares.toInt
        }
        
        val xmlWeight = (poolNode \ POOL_WEIGHT_PROPERTY).text
        if(xmlWeight != "")
        {
          weight = xmlWeight.toInt
        }

        val pool = new Pool(poolName,schedulingMode,minShares,weight)
        pools += pool
        poolNameToPool(poolName) = pool
        logInfo("Create new pool with name:%s,schedulingMode:%s,minShares:%d,weight:%d".format(poolName,schedulingMode,minShares,weight))
      }
    }

      if(!poolNameToPool.contains(POOL_DEFAULT_POOL_NAME)) 
      {
        val pool = new Pool(POOL_DEFAULT_POOL_NAME, POOL_DEFAULT_SCHEDULING_MODE,POOL_DEFAULT_MINIMUM_SHARES,POOL_DEFAULT_WEIGHT)
        pools += pool
        poolNameToPool(POOL_DEFAULT_POOL_NAME) = pool
        logInfo("Create default pool with name:%s,schedulingMode:%s,minShares:%d,weight:%d".format(POOL_DEFAULT_POOL_NAME,POOL_DEFAULT_SCHEDULING_MODE,POOL_DEFAULT_MINIMUM_SHARES,POOL_DEFAULT_WEIGHT))
      }    
  }
  
  override def addTaskSetManager(manager: TaskSetManager) {
    var poolName = POOL_DEFAULT_POOL_NAME
    if(manager.taskSet.properties != null)
    {
      poolName = manager.taskSet.properties.getProperty(POOL_FAIR_SCHEDULER_PROPERTIES,POOL_DEFAULT_POOL_NAME)
      if(!poolNameToPool.contains(poolName))
      {
        //we will create a new pool that user has configured in app,but not contained in xml file  
        val pool = new Pool(poolName,POOL_DEFAULT_SCHEDULING_MODE,POOL_DEFAULT_MINIMUM_SHARES,POOL_DEFAULT_WEIGHT)
        pools += pool
        poolNameToPool(poolName) = pool
        logInfo("Create pool with name:%s,schedulingMode:%s,minShares:%d,weight:%d".format(poolName,POOL_DEFAULT_SCHEDULING_MODE,POOL_DEFAULT_MINIMUM_SHARES,POOL_DEFAULT_WEIGHT)) 
      }
    }
    poolNameToPool(poolName).addTaskSetManager(manager)
    logInfo("Added task set " + manager.taskSet.id + " tasks to pool "+poolName)        
  }
  
  override def removeTaskSetManager(manager: TaskSetManager) {
    
    var poolName = POOL_DEFAULT_POOL_NAME
    if(manager.taskSet.properties != null)
    {
      poolName = manager.taskSet.properties.getProperty(POOL_FAIR_SCHEDULER_PROPERTIES,POOL_DEFAULT_POOL_NAME)
    }
    logInfo("Remove TaskSet %s from pool %s".format(manager.taskSet.id,poolName))
    val pool = poolNameToPool(poolName)
    pool.removeTaskSetManager(manager)
    pool.setRunningTasks(pool.getRunningTasks() - manager.getRunningTasks())
    
  }
  
  override def taskFinished(manager: TaskSetManager) {
    var poolName = POOL_DEFAULT_POOL_NAME
    if(manager.taskSet.properties != null)
    {
      poolName = manager.taskSet.properties.getProperty(POOL_FAIR_SCHEDULER_PROPERTIES,POOL_DEFAULT_POOL_NAME)
    }
    val pool = poolNameToPool(poolName)
    pool.setRunningTasks(pool.getRunningTasks() - 1)
    manager.setRunningTasks(manager.getRunningTasks() - 1)
  }
  
  override def removeExecutor(executorId: String, host: String) {
    for (pool <- pools) {
      pool.removeExecutor(executorId,host)      
    }    
  }
  
  override def receiveOffer(execId: String,host:String,avaiableCpus:Double):Option[TaskDescription] =
  {

   val sortedPools = pools.sortWith(poolScheduleAlgorithm.comparator)
   for(pool <- sortedPools)
   {
     logDebug("poolName:%s,tasksetNum:%d,minShares:%d,runningTasks:%d".format(pool.poolName,pool.activeTaskSetsQueue.length,pool.getMinShare(),pool.getRunningTasks()))  
   }
   for (pool <- sortedPools)
   {
     val task = pool.receiveOffer(execId,host,avaiableCpus)
     if(task != None)
     {
         pool.setRunningTasks(pool.getRunningTasks() + 1)
         return task
     }
   }
   return None
  }
  
  override def checkSpeculatableTasks(): Boolean = 
  {
    var shouldRevive = false
    for (pool <- pools) 
    {
      shouldRevive |= pool.checkSpeculatableTasks()
    }
    return shouldRevive
  }

 }

package spark.scheduler.cluster

import java.io.{File, FileInputStream, FileOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks._
import scala.xml._

import spark.Logging

/**
 * A Fair Implementation of the TaskSetQueuesManager
 * 
 * The current implementation makes the following assumptions: A pool has a fixed configuration of weight. 
 * Within a pool, it just uses FIFO.
 * Also, currently we assume that pools are statically defined
 * We currently don't support min shares
 */
private[spark] class FairTaskSetQueuesManager extends TaskSetQueuesManager with Logging {
  
  val schedulerAllocFile = System.getProperty("spark.fairscheduler.allocation.file","unspecified")  
  val poolNameToPool= new HashMap[String, Pool]
  var pools = new ArrayBuffer[Pool]
  
  loadPoolProperties()
  
  def loadPoolProperties() {
    //first check if the file exists
    val file = new File(schedulerAllocFile)
    if(!file.exists()) {
    //if file does not exist, we just create 1 pool, default
      val pool = new Pool("default",100)
      pools += pool
      poolNameToPool("default") = pool
      logInfo("Created a default pool with weight = 100")
    }
    else {
      val xml = XML.loadFile(file)
      for (poolNode <- (xml \\ "pool")) {
        if((poolNode \ "weight").text != ""){
          val pool = new Pool((poolNode \ "@name").text,(poolNode \ "weight").text.toInt)
          pools += pool
          poolNameToPool((poolNode \ "@name").text) = pool
          logInfo("Created pool "+ pool.name +"with weight = "+pool.weight)
        } else {
          val pool = new Pool((poolNode \ "@name").text,100)
          pools += pool
          poolNameToPool((poolNode \ "@name").text) = pool
          logInfo("Created pool "+ pool.name +"with weight = 100")
        }
      }
      if(!poolNameToPool.contains("default")) {
        val pool = new Pool("default", 100)
        pools += pool
        poolNameToPool("default") = pool
        logInfo("Created a default pool with weight = 100")
      }
        
    }    
  }
  
  override def addTaskSetManager(manager: TaskSetManager) {
    var poolName = "default"
    if(manager.taskSet.properties != null)  
      poolName = manager.taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")
    if(poolNameToPool.contains(poolName))          
      poolNameToPool(poolName).activeTaskSetsQueue += manager          
    else
      poolNameToPool("default").activeTaskSetsQueue += manager
    logInfo("Added task set " + manager.taskSet.id + " tasks to pool "+poolName)
        
  }
  
  override def removeTaskSetManager(manager: TaskSetManager) {
    var poolName = "default"
    if(manager.taskSet.properties != null)  
      poolName = manager.taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")
    if(poolNameToPool.contains(poolName))
      poolNameToPool(poolName).activeTaskSetsQueue -= manager
    else
      poolNameToPool("default").activeTaskSetsQueue -= manager      
  }
  
  override def taskFinished(manager: TaskSetManager) {
    var poolName = "default"
    if(manager.taskSet.properties != null)  
      poolName = manager.taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")
    if(poolNameToPool.contains(poolName))
      poolNameToPool(poolName).numRunningTasks -= 1
    else
      poolNameToPool("default").numRunningTasks -= 1      
  }
  
  override def removeExecutor(executorId: String, host: String) {
    for (pool <- pools) {
      pool.activeTaskSetsQueue.foreach(_.executorLost(executorId, host))      
    }    
  }
  
  /**
   * This is the comparison function used for sorting to determine which 
   * pool to allocate next based on fairness.
   * The algorithm is as follows: we sort by the pool's running tasks to weight ratio
   * (pools number running tast / pool's weight)
   */
  def poolFairCompFn(pool1: Pool, pool2: Pool): Boolean = {    
    val tasksToWeightRatio1 = pool1.numRunningTasks.toDouble / pool1.weight.toDouble
    val tasksToWeightRatio2 = pool2.numRunningTasks.toDouble / pool2.weight.toDouble
    var res = Math.signum(tasksToWeightRatio1 - tasksToWeightRatio2)
    if (res == 0) {
      //Jobs are tied in fairness ratio. We break the tie by name
      res = pool1.name.compareTo(pool2.name)
    }
    if (res < 0)
      return true
    else
      return false
  }
  
  override def receiveOffer(tasks: Seq[ArrayBuffer[TaskDescription]], offers: Seq[WorkerOffer]): Seq[Seq[String]] = {
    val taskSetIds = offers.map(o => new ArrayBuffer[String](o.cores))
    val availableCpus = offers.map(o => o.cores).toArray
    var launchedTask = false
      
    for (i <- 0 until offers.size) { //we loop through the list of offers
      val execId = offers(i).executorId
      val host = offers(i).hostname
      var breakOut = false
      while(availableCpus(i) > 0 && !breakOut) {
        breakable{
          launchedTask = false          
          for (pool <- pools.sortWith(poolFairCompFn)) { //we loop through the list of pools
            if(!pool.activeTaskSetsQueue.isEmpty) {
              //sort the tasksetmanager in the pool
              pool.activeTaskSetsQueue.sortBy(m => (m.taskSet.priority, m.taskSet.stageId))
              for(manager <- pool.activeTaskSetsQueue) { //we loop through the activeTaskSets in this pool
                //Make an offer
                manager.slaveOffer(execId, host, availableCpus(i)) match {
                    case Some(task) =>
                      tasks(i) += task
                      taskSetIds(i) += manager.taskSet.id
                      availableCpus(i) -= 1
                      pool.numRunningTasks += 1
                      launchedTask = true
                      logInfo("launched task for pool"+pool.name);
                      break
                    case None => {}
                }
              }
            }
          }
          //If there is not one pool that can assign the task then we have to exit the outer loop and continue to the next offer
          if(!launchedTask){
            breakOut = true
          }              
        }
      }
    }
    return taskSetIds    
  }

  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (pool <- pools) {
      for (ts <- pool.activeTaskSetsQueue) {
        shouldRevive |= ts.checkSpeculatableTasks()
      }       
    }
    return shouldRevive
  }
}

/**
 * An internal representation of a pool. It contains an ArrayBuffer of TaskSets and also weight
 */
class Pool(val name: String, val weight: Int)
{
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]
  var numRunningTasks: Int = 0 
}
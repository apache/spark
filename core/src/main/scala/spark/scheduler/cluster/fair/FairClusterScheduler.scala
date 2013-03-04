package spark.scheduler.cluster.fair

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.{TimerTask, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks._
import scala.xml._

import spark._
import spark.TaskState.TaskState
import spark.scheduler._
import spark.scheduler.cluster._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import scala.io.Source

/**
 * An implementation of a fair TaskScheduler, for running tasks on a cluster. Clients should first call
 * start(), then submit task sets through the runTasks method.
 * 
 * The current implementation makes the following assumptions: A pool has a fixed configuration of weight. 
 * Within a pool, it just uses FIFO.
 * Also, currently we assume that pools are statically defined
 * We currently don't support min shares
 */
private[spark] class FairClusterScheduler(override val sc: SparkContext)
  extends ClusterScheduler(sc)
  with Logging {
 
  
  val schedulerAllocFile = System.getProperty("mapred.fairscheduler.allocation.file","unspecified")
  
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
  
  def taskFinished(manager: TaskSetManager) {
    var poolName = "default"
    if(manager.taskSet.properties != null)  
      poolName = manager.taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")

    this.synchronized {
      //have to check that poolName exists
      if(poolNameToPool.contains(poolName))
      {
        poolNameToPool(poolName).numRunningTasks -= 1
      }
      else
      {
        poolNameToPool("default").numRunningTasks -= 1
      }
    }
  }
  
  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    
        
    var poolName = "default"
    if(taskSet.properties != null)  
      poolName = taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")
    
    this.synchronized {
      if(poolNameToPool.contains(poolName))
      {      
        val manager = new FairTaskSetManager(this, taskSet)
        poolNameToPool(poolName).activeTaskSetsQueue += manager
        activeTaskSets(taskSet.id) = manager
        //activeTaskSetsQueue += manager
        taskSetTaskIds(taskSet.id) = new HashSet[Long]()
        logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks to pool "+poolName)
      }
      else //If the pool name does not exists, where do we put them? We put them in default
      {
        val manager = new FairTaskSetManager(this, taskSet)
        poolNameToPool("default").activeTaskSetsQueue += manager
        activeTaskSets(taskSet.id) = manager
        //activeTaskSetsQueue += manager
        taskSetTaskIds(taskSet.id) = new HashSet[Long]()
        logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks to pool default")
      }
      if (hasReceivedTask == false) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT, STARVATION_TIMEOUT)
      }
      hasReceivedTask = true;

    }
    backend.reviveOffers()
  }

  override def taskSetFinished(manager: TaskSetManager) {
    
    var poolName = "default"
    if(manager.taskSet.properties != null)  
      poolName = manager.taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")
    
    
    this.synchronized {        
      //have to check that poolName exists
      if(poolNameToPool.contains(poolName))
      {
        poolNameToPool(poolName).activeTaskSetsQueue -= manager
      }
      else
      {
        poolNameToPool("default").activeTaskSetsQueue -= manager
      }
      //activeTaskSetsQueue -= manager
      activeTaskSets -= manager.taskSet.id
      taskIdToTaskSetId --= taskSetTaskIds(manager.taskSet.id)
      taskIdToExecutorId --= taskSetTaskIds(manager.taskSet.id)
      taskSetTaskIds.remove(manager.taskSet.id)
    }
    //backend.reviveOffers()
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

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a fair manner so
   * that tasks are balanced across the cluster.
   */
  override def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = {
    synchronized {
      SparkEnv.set(sc.env)
      // Mark each slave as alive and remember its hostname
      for (o <- offers) {
        executorIdToHost(o.executorId) = o.hostname
        if (!executorsByHost.contains(o.hostname)) {
          executorsByHost(o.hostname) = new HashSet()
        }        
      }
      // Build a list of tasks to assign to each slave
      val tasks = offers.map(o => new ArrayBuffer[TaskDescription](o.cores))
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
//                val manager = pool.activeTaskSetsQueue.head
                  //Make an offer
                  manager.slaveOffer(execId, host, availableCpus(i)) match {
                      case Some(task) =>
                        tasks(i) += task
                        val tid = task.taskId
                        taskIdToTaskSetId(tid) = manager.taskSet.id
                        taskSetTaskIds(manager.taskSet.id) += tid
                        taskIdToExecutorId(tid) = execId
                        activeExecutorIds += execId
                        executorsByHost(host) += execId
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
      if (tasks.size > 0) {
        hasLaunchedTask = true
      }
      return tasks
    }
  }

  override def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var taskSetToUpdate: Option[TaskSetManager] = None
    var failedExecutor: Option[String] = None
    var taskFailed = false
    synchronized {
      try {
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
          // We lost this entire executor, so remember that it's gone
          val execId = taskIdToExecutorId(tid)
          if (activeExecutorIds.contains(execId)) {
            removeExecutor(execId)
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetId.get(tid) match {
          case Some(taskSetId) =>
            if (activeTaskSets.contains(taskSetId)) {
              taskSetToUpdate = Some(activeTaskSets(taskSetId))
            }
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(tid)
              if (taskSetTaskIds.contains(taskSetId)) {
                taskSetTaskIds(taskSetId) -= tid
              }
              taskIdToExecutorId.remove(tid)
            }
            if (state == TaskState.FAILED) {
              taskFailed = true
            }
          case None =>
            logInfo("Ignoring update from TID " + tid + " because its task set is gone")
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the task set and DAGScheduler without holding a lock on this, since that can deadlock
    if (taskSetToUpdate != None) {
      taskSetToUpdate.get.statusUpdate(tid, state, serializedData)
    }
    if (failedExecutor != None) {
      listener.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
    if (taskFailed) {
      // Also revive offers if a task had failed for some reason other than host lost
      backend.reviveOffers()
    }
  }

  // Check for speculatable tasks in all our active jobs.
  override def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      for (pool <- pools) {
        for (ts <- pool.activeTaskSetsQueue) {
          shouldRevive |= ts.checkSpeculatableTasks()
        }       
      }
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }
    
  /** Remove an executor from all our data structures and mark it as lost */
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
    }
    executorIdToHost -= executorId
    for (pool <- pools) {
      pool.activeTaskSetsQueue.foreach(_.executorLost(executorId, host))      
    }    
  }
  
}

/**
 * An internal representation of a pool. It contains an ArrayBuffer of TaskSets and also weight and minshare
 */
class Pool(val name: String, val weight: Int)
{
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]
  var numRunningTasks: Int = 0 
}

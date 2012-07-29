package spark.streaming

import spark.SparkEnv
import spark.Logging

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer

import akka.actor._
import akka.actor.Actor
import akka.actor.Actor._
import akka.util.duration._

sealed trait SchedulerMessage
case class InputGenerated(inputName: String, interval: Interval, reference: AnyRef = null) extends SchedulerMessage
case class Test extends SchedulerMessage

class Scheduler(
    ssc: SparkStreamContext, 
    inputRDSs: Array[InputRDS[_]], 
    outputRDSs: Array[RDS[_]])
extends Actor with Logging {

  class InputState (inputNames: Array[String]) {
    val inputsLeft = new HashSet[String]()
    inputsLeft ++= inputNames
    
    val startTime = System.currentTimeMillis

    def delay() = System.currentTimeMillis - startTime

    def addGeneratedInput(inputName: String) = inputsLeft -= inputName

    def areAllInputsGenerated() = (inputsLeft.size == 0)  

    override def toString(): String = {
      val left = if (inputsLeft.size == 0) "" else inputsLeft.reduceLeft(_ + ", " + _)
      return "Inputs left = [ " + left + " ]"
    }
  }


  initLogging()

  val inputNames = inputRDSs.map(_.inputName).toArray
  val inputStates = new HashMap[Interval, InputState]()
  val currentJobs = System.getProperty("spark.stream.currentJobs", "1").toInt
  val jobManager = new JobManager2(ssc, currentJobs)

  // TODO(Haoyuan): The following line is for performance test only.
  var cnt: Int = System.getProperty("spark.stream.fake.cnt", "60").toInt
  var lastInterval: Interval = null

 
    /*remote.register("SparkStreamScheduler", actorOf[Scheduler])*/
    logInfo("Registered actor on port ")

    /*jobManager.start()*/
    startStreamReceivers() 
  
  def receive = {
    case InputGenerated(inputName, interval, reference) => { 
      addGeneratedInput(inputName, interval, reference)
    }
    case Test() => logInfo("TEST PASSED")
  }

  def addGeneratedInput(inputName: String, interval: Interval, reference: AnyRef = null) {
    logInfo("Input " + inputName + " generated for interval " + interval)
    inputStates.get(interval) match {
      case None => inputStates.put(interval, new InputState(inputNames))
      case _ => 
    }
    inputStates(interval).addGeneratedInput(inputName)
    
    inputRDSs.filter(_.inputName == inputName).foreach(inputRDS => {
        inputRDS.setReference(interval.endTime, reference)
        if (inputRDS.isInstanceOf[TestInputRDS]) {
          TestInputBlockTracker.addBlocks(interval.endTime, reference)
        }
      }
    )
   
    def getNextInterval(): Option[Interval] = {
      logDebug("Last interval is " + lastInterval)
      val readyIntervals = inputStates.filter(_._2.areAllInputsGenerated).keys
      /*inputState.foreach(println)        */
      logDebug("InputState has " + inputStates.size + " intervals, " + readyIntervals.size + " ready intervals")
      return readyIntervals.find(lastInterval == null || _.beginTime == lastInterval.endTime)
    }

    var nextInterval = getNextInterval()
    var count = 0
    while(nextInterval.isDefined) {
      val inputState = inputStates.get(nextInterval.get).get
      generateRDDsForInterval(nextInterval.get)
      logInfo("Skew delay for " + nextInterval.get.endTime + " is " + (inputState.delay / 1000.0)  + " s")
      inputStates.remove(nextInterval.get)
      lastInterval = nextInterval.get
      nextInterval = getNextInterval() 
      count += 1
      /*if (nextInterval.size == 0 && inputState.size > 0) {
        logDebug("Next interval not ready, pending intervals " + inputState.size) 
      }*/
    }
    logDebug("RDDs generated for " + count  + " intervals") 
     
   /*
   if (inputState(interval).areAllInputsGenerated) {
      generateRDDsForInterval(interval)
      lastInterval = interval
      inputState.remove(interval)
    } else {
      logInfo("All inputs not generated for interval " + interval)
    }
    */
  }

  def generateRDDsForInterval (interval: Interval) {
    logInfo("Generating RDDs for interval " + interval) 
    outputRDSs.foreach(outputRDS => {
        if (!outputRDS.isInitialized) outputRDS.initialize(interval)
        outputRDS.generateJob(interval.endTime) match {
          case Some(job) => submitJob(job)
          case None => 
        }
      }
    )
    // TODO(Haoyuan): This comment is for performance test only.
    if (System.getProperty("spark.fake", "false") == "true" || System.getProperty("spark.stream.fake", "false") == "true") {
      cnt -= 1
      if (cnt <= 0) {
        logInfo("My time is up! " + cnt)
        System.exit(1)
      }
    }
  }

  def submitJob(job: Job) {
    logInfo("Submitting " + job + " to JobManager")
    /*jobManager ! RunJob(job)*/
    jobManager.runJob(job)
  }

  def startStreamReceivers() {
    val testStreamReceiverNames = new ArrayBuffer[(String, Long)]() 
    inputRDSs.foreach (inputRDS => {
      inputRDS match {
        case fileInputRDS: FileInputRDS => { 
          val fileStreamReceiver = new FileStreamReceiver(
            fileInputRDS.inputName, 
            fileInputRDS.directory, 
            fileInputRDS.batchDuration.asInstanceOf[LongTime].milliseconds)
          fileStreamReceiver.start()
        }
        case networkInputRDS: NetworkInputRDS[_] => {
          val networkStreamReceiver = new NetworkStreamReceiver(
            networkInputRDS.inputName, 
            networkInputRDS.batchDuration, 
            0,
            ssc,
            if (ssc.tempDir == null) null else ssc.tempDir.toString)
          networkStreamReceiver.start()
        }
        case testInputRDS: TestInputRDS => {
          testStreamReceiverNames += 
            ((testInputRDS.inputName, testInputRDS.batchDuration.asInstanceOf[LongTime].milliseconds))
          }
      }
    })
    if (testStreamReceiverNames.size > 0) {
      /*val testStreamCoordinator = new TestStreamCoordinator(testStreamReceiverNames.toArray)*/
      /*testStreamCoordinator.start()*/
      val actor = ssc.actorSystem.actorOf(
        Props(new TestStreamCoordinator(testStreamReceiverNames.toArray)),
        name = "TestStreamCoordinator")
    }
  }
}


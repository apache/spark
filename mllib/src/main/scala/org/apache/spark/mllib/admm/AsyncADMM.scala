package org.apache.spark.mllib.admm

import breeze.linalg.norm
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import akka.actor._
import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.deploy.worker.Worker
import scala.language.postfixOps
import java.util.UUID
import java.util

import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import org.apache.spark.mllib.admm.InternalMessages.PingPong
import scala.collection.mutable

case class AsyncSubProblem(points: Array[BV], labels: Array[Double], comm: WorkerCommunication)

// fuck actors
class WorkerCommunicationHack {
  var ref: WorkerCommunication = null
}

object InternalMessages {
  class WakeupMsg
  class PingPong
}

class WorkerCommunication(val address: String, val hack: WorkerCommunicationHack) extends Actor {
  hack.ref = this
  var others = new mutable.HashMap[Int, ActorSelection]

  def receive = {
    case ppm: PingPong => {
      println("new message from "+sender)
      sender ! "gotit!"
    }
    case m: InternalMessages.WakeupMsg => { println("activated local!"); sender ! "yo" }
    case s: String => println(s)
    case _ => println("hello, world!")
  }

  def shuttingDown: Receive = {
    case _ => println("GOT SHUTDOWN!")
  }

  def connectToOthers(allHosts: Array[String]) {
    var i = 0
    for(host <- allHosts) {
      if(!host.equals(address)) {
        others.put(i, context.actorSelection(allHosts(i)))
        /*
        others(i) ! new PingPong
        implicit val timeout = Timeout(15 seconds)

        val f = others(i).resolveOne()
        Await.ready(f, Duration.Inf)
        println(f.value.get.get)
        */
      }
      i += 1
    }
  }

  def sendPingPongs() {
    for(other <- others.values) {
      other ! new PingPong
    }
  }
}

object AsyncADMMwithSGD {
  def train(input: RDD[(Double, Vector)],
            numADMMIterations: Int,
            gradient: BVGradient,
            initialWeights: Vector) = {
    val subProblems: RDD[BSPSubProblem] = input.mapPartitions {
      iter =>
        val localData = iter.toArray
        val points = localData.map {
          case (y, x) => x.toBreeze
        }
        val labels = localData.map {
          case (y, x) => y
        }
        Iterator(BSPSubProblem(points, labels))
    }

    val workers: RDD[(WorkerCommunication)] = input.mapPartitions {
      iter =>
        val workerName = UUID.randomUUID().toString
        val address = Worker.HACKakkaHost+workerName
        val hack = new WorkerCommunicationHack()
        println(address)
        val aref= Worker.HACKworkerActorSystem.actorOf(Props(new WorkerCommunication(address, hack)), workerName)
        implicit val timeout = Timeout(15 seconds)

        val f = aref ? new InternalMessages.WakeupMsg
        Await.result(f, timeout.duration).asInstanceOf[String]

        Iterator(hack.ref)
    }

    val addresses = workers.map { w => w.address }.collect()

    workers.foreach {
      w => w.connectToOthers(addresses)
    }

    workers.foreach {
      w => w.sendPingPongs()
    }

    val asyncProblems: RDD[AsyncSubProblem] = workers.zipPartitions(subProblems) {
      (wit, pit) => {
        val w = wit.next()
        val p = pit.next()
        Iterator(new AsyncSubProblem(p.points, p.labels, w))
      }
    }

    Thread.sleep(1000)

    null
  }
}

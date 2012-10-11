package spark

import org.scalatest.FunSuite

import scala.collection.mutable.HashMap

import akka.actor._
import akka.dispatch._
import akka.pattern.ask
import akka.remote._
import akka.util.Duration
import akka.util.Timeout
import akka.util.duration._

class CacheTrackerSuite extends FunSuite {
  // Send a message to an actor and wait for a reply, in a blocking manner
  private def ask(actor: ActorRef, message: Any): Any = {
    try {
      val timeout = 10.seconds
      val future = actor.ask(message)(timeout)
      return Await.result(future, timeout)
    } catch {
      case e: Exception =>
        throw new SparkException("Error communicating with actor", e)
    }
  }

  test("CacheTrackerActor slave initialization & cache status") {
    //System.setProperty("spark.master.port", "1345")
    val initialSize = 2L << 20

    val actorSystem = ActorSystem("test")
    val tracker = actorSystem.actorOf(Props[CacheTrackerActor])

    assert(ask(tracker, SlaveCacheStarted("host001", initialSize)) === true)

    assert(ask(tracker, GetCacheStatus) === Seq(("host001", 2097152L, 0L)))

    assert(ask(tracker, StopCacheTracker) === true)
    
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  test("RegisterRDD") {
    //System.setProperty("spark.master.port", "1345")
    val initialSize = 2L << 20

    val actorSystem = ActorSystem("test")
    val tracker = actorSystem.actorOf(Props[CacheTrackerActor])

    assert(ask(tracker, SlaveCacheStarted("host001", initialSize)) === true)

    assert(ask(tracker, RegisterRDD(1, 3)) === true)
    assert(ask(tracker, RegisterRDD(2, 1)) === true)

    assert(getCacheLocations(tracker) === Map(1 -> List(Nil, Nil, Nil), 2 -> List(Nil)))

    assert(ask(tracker, StopCacheTracker) === true)
    
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  test("AddedToCache") {
    //System.setProperty("spark.master.port", "1345")
    val initialSize = 2L << 20

    val actorSystem = ActorSystem("test")
    val tracker = actorSystem.actorOf(Props[CacheTrackerActor])

    assert(ask(tracker, SlaveCacheStarted("host001", initialSize)) === true)

    assert(ask(tracker, RegisterRDD(1, 2)) === true)
    assert(ask(tracker, RegisterRDD(2, 1)) === true)

    assert(ask(tracker, AddedToCache(1, 0, "host001", 2L << 15)) === true)
    assert(ask(tracker, AddedToCache(1, 1, "host001", 2L << 11)) === true)
    assert(ask(tracker, AddedToCache(2, 0, "host001", 3L << 10)) === true)

    assert(ask(tracker, GetCacheStatus) === Seq(("host001", 2097152L, 72704L)))

    assert(getCacheLocations(tracker) === 
      Map(1 -> List(List("host001"), List("host001")), 2 -> List(List("host001"))))

    assert(ask(tracker, StopCacheTracker) === true)
    
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  test("DroppedFromCache") {
    //System.setProperty("spark.master.port", "1345")
    val initialSize = 2L << 20

    val actorSystem = ActorSystem("test")
    val tracker = actorSystem.actorOf(Props[CacheTrackerActor])

    assert(ask(tracker, SlaveCacheStarted("host001", initialSize)) === true)

    assert(ask(tracker, RegisterRDD(1, 2)) === true)
    assert(ask(tracker, RegisterRDD(2, 1)) === true)

    assert(ask(tracker, AddedToCache(1, 0, "host001", 2L << 15)) === true)
    assert(ask(tracker, AddedToCache(1, 1, "host001", 2L << 11)) === true)
    assert(ask(tracker, AddedToCache(2, 0, "host001", 3L << 10)) === true)

    assert(ask(tracker, GetCacheStatus) === Seq(("host001", 2097152L, 72704L)))
    assert(getCacheLocations(tracker) ===
      Map(1 -> List(List("host001"), List("host001")), 2 -> List(List("host001"))))

    assert(ask(tracker, DroppedFromCache(1, 1, "host001", 2L << 11)) === true)

    assert(ask(tracker, GetCacheStatus) === Seq(("host001", 2097152L, 68608L)))
    assert(getCacheLocations(tracker) ===
      Map(1 -> List(List("host001"),List()), 2 -> List(List("host001"))))

    assert(ask(tracker, StopCacheTracker) === true)
    
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  /**
   * Helper function to get cacheLocations from CacheTracker
   */
  def getCacheLocations(tracker: ActorRef): HashMap[Int, List[List[String]]] = {
    val answer = ask(tracker, GetCacheLocations).asInstanceOf[HashMap[Int, Array[List[String]]]]
    answer.map { case (i, arr) => (i, arr.toList) }
  }
}

package spark

import org.scalatest.FunSuite
import collection.mutable.HashMap

class CacheTrackerSuite extends FunSuite {

  test("CacheTrackerActor slave initialization & cache status") {
    System.setProperty("spark.master.port", "1345")
    val initialSize = 2L << 20

    val tracker = new CacheTrackerActor
    tracker.start()

    tracker !? SlaveCacheStarted("host001", initialSize)

    assert(tracker !? GetCacheStatus == Seq(("host001", 2097152L, 0L)))

    tracker !? StopCacheTracker
  }

  test("RegisterRDD") {
    System.setProperty("spark.master.port", "1345")
    val initialSize = 2L << 20

    val tracker = new CacheTrackerActor
    tracker.start()

    tracker !? SlaveCacheStarted("host001", initialSize)

    tracker !? RegisterRDD(1, 3)
    tracker !? RegisterRDD(2, 1)

    assert(getCacheLocations(tracker) == Map(1 -> List(List(), List(), List()), 2 -> List(List())))

    tracker !? StopCacheTracker
  }

  test("AddedToCache") {
    System.setProperty("spark.master.port", "1345")
    val initialSize = 2L << 20

    val tracker = new CacheTrackerActor
    tracker.start()

    tracker !? SlaveCacheStarted("host001", initialSize)

    tracker !? RegisterRDD(1, 2)
    tracker !? RegisterRDD(2, 1)

    tracker !? AddedToCache(1, 0, "host001", 2L << 15)
    tracker !? AddedToCache(1, 1, "host001", 2L << 11)
    tracker !? AddedToCache(2, 0, "host001", 3L << 10)

    assert(tracker !? GetCacheStatus == Seq(("host001", 2097152L, 72704L)))

    assert(getCacheLocations(tracker) == Map(1 -> List(List("host001"), List("host001")), 2 -> List(List("host001"))))

    tracker !? StopCacheTracker
  }

  test("DroppedFromCache") {
    System.setProperty("spark.master.port", "1345")
    val initialSize = 2L << 20

    val tracker = new CacheTrackerActor
    tracker.start()

    tracker !? SlaveCacheStarted("host001", initialSize)

    tracker !? RegisterRDD(1, 2)
    tracker !? RegisterRDD(2, 1)

    tracker !? AddedToCache(1, 0, "host001", 2L << 15)
    tracker !? AddedToCache(1, 1, "host001", 2L << 11)
    tracker !? AddedToCache(2, 0, "host001", 3L << 10)

    assert(tracker !? GetCacheStatus == Seq(("host001", 2097152L, 72704L)))
    assert(getCacheLocations(tracker) == Map(1 -> List(List("host001"), List("host001")), 2 -> List(List("host001"))))

    tracker !? DroppedFromCache(1, 1, "host001", 2L << 11)

    assert(tracker !? GetCacheStatus == Seq(("host001", 2097152L, 68608L)))
    assert(getCacheLocations(tracker) == Map(1 -> List(List("host001"),List()), 2 -> List(List("host001"))))

    tracker !? StopCacheTracker
  }

  /**
   * Helper function to get cacheLocations from CacheTracker
   */
  def getCacheLocations(tracker: CacheTrackerActor) = tracker !? GetCacheLocations match {
    case h: HashMap[_, _] => h.asInstanceOf[HashMap[Int, Array[List[String]]]].map {
      case (i, arr) => (i -> arr.toList)
    }
  }
}

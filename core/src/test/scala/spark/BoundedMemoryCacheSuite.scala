package spark

import org.scalatest.FunSuite
import org.scalatest.PrivateMethodTester

class BoundedMemoryCacheSuite extends FunSuite with PrivateMethodTester {
  test("constructor test") {
    val cache = new BoundedMemoryCache(60)
    expect(60)(cache.getCapacity)
  }

  test("caching") {
    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case 
    val oldArch = System.setProperty("os.arch", "amd64")
    val oldOops = System.setProperty("spark.test.useCompressedOops", "true")
    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()

    val cache = new BoundedMemoryCache(60) {
      //TODO sorry about this, but there is not better way how to skip 'cacheTracker.dropEntry'
      override protected def reportEntryDropped(datasetId: Any, partition: Int, entry: Entry) {
        logInfo("Dropping key (%s, %d) of size %d to make space".format(datasetId, partition, entry.size))
      }
    }
    //should be OK
    expect(CachePutSuccess(56))(cache.put("1", 0, "Meh"))

    //we cannot add this to cache (there is not enough space in cache) & we cannot evict the only value from
    //cache because it's from the same dataset
    expect(CachePutFailure())(cache.put("1", 1, "Meh"))

    //should be OK, dataset '1' can be evicted from cache
    expect(CachePutSuccess(56))(cache.put("2", 0, "Meh"))

    //should fail, cache should obey it's capacity
    expect(CachePutFailure())(cache.put("3", 0, "Very_long_and_useless_string"))

    if (oldArch != null) {
      System.setProperty("os.arch", oldArch)
    } else {
      System.clearProperty("os.arch")
    }

    if (oldOops != null) {
      System.setProperty("spark.test.useCompressedOops", oldOops)
    } else {
      System.clearProperty("spark.test.useCompressedOops")
    }
  }
}

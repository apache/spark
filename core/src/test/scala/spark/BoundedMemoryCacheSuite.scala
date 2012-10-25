package spark

import org.scalatest.FunSuite
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.ShouldMatchers

// TODO: Replace this with a test of MemoryStore
class BoundedMemoryCacheSuite extends FunSuite with PrivateMethodTester with ShouldMatchers {
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

    // NOTE: The String class definition changed in JDK 7 to exclude the int fields count and length
    // This means that the size of strings will be lesser by 8 bytes in JDK 7 compared to JDK 6.
    // http://mail.openjdk.java.net/pipermail/core-libs-dev/2012-May/010257.html
    // Work around to check for either.

    //should be OK
    cache.put("1", 0, "Meh") should (equal (CachePutSuccess(56)) or equal (CachePutSuccess(48)))

    //we cannot add this to cache (there is not enough space in cache) & we cannot evict the only value from
    //cache because it's from the same dataset
    expect(CachePutFailure())(cache.put("1", 1, "Meh"))

    //should be OK, dataset '1' can be evicted from cache
    cache.put("2", 0, "Meh") should (equal (CachePutSuccess(56)) or equal (CachePutSuccess(48)))

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

package spark

import org.scalatest.FunSuite

class BoundedMemoryCacheTest extends FunSuite {
  test("constructor test") {
    val cache = new BoundedMemoryCache(40)
    expect(40)(cache.getCapacity)
  }

  test("caching") {
    val cache = new BoundedMemoryCache(40) {
      //TODO sorry about this, but there is not better way how to skip 'cacheTracker.dropEntry'
      override protected def reportEntryDropped(datasetId: Any, partition: Int, entry: Entry) {
        logInfo("Dropping key (%s, %d) of size %d to make space".format(datasetId, partition, entry.size))
      }
    }
    //should be OK
    expect(CachePutSuccess(30))(cache.put("1", 0, "Meh"))

    //we cannot add this to cache (there is not enough space in cache) & we cannot evict the only value from
    //cache because it's from the same dataset
    expect(CachePutFailure())(cache.put("1", 1, "Meh"))

    //should be OK, dataset '1' can be evicted from cache
    expect(CachePutSuccess(30))(cache.put("2", 0, "Meh"))

    //should fail, cache should obey it's capacity
    expect(CachePutFailure())(cache.put("3", 0, "Very_long_and_useless_string"))
  }
}

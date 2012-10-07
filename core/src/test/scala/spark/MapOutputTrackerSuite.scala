package spark

import org.scalatest.FunSuite

class MapOutputTrackerSuite extends FunSuite {
  test("compressSize") {
    assert(MapOutputTracker.compressSize(0L) === 0)
    assert(MapOutputTracker.compressSize(1L) === 0)
    assert(MapOutputTracker.compressSize(2L) === 8)
    assert(MapOutputTracker.compressSize(10L) === 25)
    assert((MapOutputTracker.compressSize(1000000L) & 0xFF) === 145)
    assert((MapOutputTracker.compressSize(1000000000L) & 0xFF) === 218)
    // This last size is bigger than we can encode in a byte, so check that we just return 255
    assert((MapOutputTracker.compressSize(1000000000000000000L) & 0xFF) === 255)
  }

  test("decompressSize") {
    assert(MapOutputTracker.decompressSize(0) === 1)
    for (size <- Seq(2L, 10L, 100L, 50000L, 1000000L, 1000000000L)) {
      val size2 = MapOutputTracker.decompressSize(MapOutputTracker.compressSize(size))
      assert(size2 >= 0.99 * size && size2 <= 1.11 * size,
        "size " + size + " decompressed to " + size2 + ", which is out of range")
    }
  }
}

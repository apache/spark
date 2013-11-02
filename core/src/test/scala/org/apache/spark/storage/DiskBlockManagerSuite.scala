package org.apache.spark.storage

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import java.io.{FileWriter, File}
import java.nio.file.Files
import scala.collection.mutable

class DiskBlockManagerSuite extends FunSuite with BeforeAndAfterEach {

  val rootDir0 = Files.createTempDirectory("disk-block-manager-suite-0")
  val rootDir1 = Files.createTempDirectory("disk-block-manager-suite-1")
  val rootDirs = rootDir0.getFileName + "," + rootDir1.getFileName
  println("Created root dirs: " + rootDirs)

  val shuffleBlockManager = new ShuffleBlockManager(null) {
    var idToSegmentMap = mutable.Map[ShuffleBlockId, FileSegment]()
    override def getBlockLocation(id: ShuffleBlockId) = idToSegmentMap(id)
  }

  var diskBlockManager: DiskBlockManager = _

  override def beforeEach() {
    diskBlockManager = new DiskBlockManager(shuffleBlockManager, rootDirs)
    shuffleBlockManager.idToSegmentMap.clear()
  }

  test("basic block creation") {
    val blockId = new TestBlockId("test")
    assertSegmentEquals(blockId, blockId.name, 0, 0)

    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
    assertSegmentEquals(blockId, blockId.name, 0, 10)

    newFile.delete()
  }

  test("block appending") {
    val blockId = new TestBlockId("test")
    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 15)
    assertSegmentEquals(blockId, blockId.name, 0, 15)
    val newFile2 = diskBlockManager.getFile(blockId)
    assert(newFile === newFile2)
    writeToFile(newFile2, 12)
    assertSegmentEquals(blockId, blockId.name, 0, 27)
    newFile.delete()
  }

  test("block remapping") {
    val filename = "test"
    val blockId0 = new ShuffleBlockId(1, 2, 3)
    val newFile = diskBlockManager.getFile(filename)
    writeToFile(newFile, 15)
    shuffleBlockManager.idToSegmentMap(blockId0) = new FileSegment(newFile, 0, 15)
    assertSegmentEquals(blockId0, filename, 0, 15)

    val blockId1 = new ShuffleBlockId(1, 2, 4)
    val newFile2 = diskBlockManager.getFile(filename)
    writeToFile(newFile2, 12)
    shuffleBlockManager.idToSegmentMap(blockId1) = new FileSegment(newFile, 15, 12)
    assertSegmentEquals(blockId1, filename, 15, 12)

    assert(newFile === newFile2)
    newFile.delete()
  }

  def assertSegmentEquals(blockId: BlockId, filename: String, offset: Int, length: Int) {
    val segment = diskBlockManager.getBlockLocation(blockId)
    assert(segment.file.getName === filename)
    assert(segment.offset === offset)
    assert(segment.length === length)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }
}

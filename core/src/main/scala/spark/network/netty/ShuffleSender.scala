package spark.network.netty

import java.io.File

import spark.Logging


private[spark] class ShuffleSender(val port: Int, val pResolver: PathResolver) extends Logging {
  val server = new FileServer(pResolver)

  Runtime.getRuntime().addShutdownHook(
    new Thread() {
      override def run() {
        server.stop()
      }
    }
  )

  def start() {
    server.run(port)
  }
}


private[spark] object ShuffleSender {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: ShuffleSender <port> <subDirsPerLocalDir> <list of shuffle_block_directories>")
      System.exit(1)
    }

    val port = args(0).toInt
    val subDirsPerLocalDir = args(1).toInt
    val localDirs = args.drop(2).map(new File(_))

    val pResovler = new PathResolver {
      override def getAbsolutePath(blockId: String): String = {
        if (!blockId.startsWith("shuffle_")) {
          throw new Exception("Block " + blockId + " is not a shuffle block")
        }
        // Figure out which local directory it hashes to, and which subdirectory in that
        val hash = math.abs(blockId.hashCode)
        val dirId = hash % localDirs.length
        val subDirId = (hash / localDirs.length) % subDirsPerLocalDir
        val subDir = new File(localDirs(dirId), "%02x".format(subDirId))
        val file = new File(subDir, blockId)
        return file.getAbsolutePath
      }
    }
    val sender = new ShuffleSender(port, pResovler)

    sender.start()
  }
}

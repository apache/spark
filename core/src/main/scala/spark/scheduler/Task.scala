package spark.scheduler

import scala.collection.mutable.HashMap
import spark.HttpFileServer
import spark.Utils
import java.io.File

/**
 * A task to execute on a worker node.
 */
abstract class Task[T](val stageId: Int) extends Serializable {
  def run(attemptId: Long): T
  def preferredLocations: Seq[String] = Nil

  var generation: Long = -1   // Map output tracker generation. Will be set by TaskScheduler.
  
  // Stores file dependencies for this task. 
  var fileSet : HashMap[String, Long] = new HashMap[String, Long]()
  
  // Downloads all file dependencies from the Master file server
  def downloadFileDependencies(currentFileSet : HashMap[String, Long]) {
    // Find files that either don't exist or have an earlier timestamp
    val missingFiles = fileSet.filter { case(k,v) => 
      !currentFileSet.isDefinedAt(k) || currentFileSet(k) <= v 
    }
    // Fetch each missing file
    missingFiles.foreach { case (k,v) => 
      Utils.fetchFile(k, new File(System.getProperty("user.dir")))
      currentFileSet(k) = v
    }
  }
  
}

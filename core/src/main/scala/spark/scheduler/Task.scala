package spark.scheduler

import scala.collection.mutable.{HashMap}
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
  
  // Stores jar and file dependencies for this task. 
  var fileSet : HashMap[String, Long] = new HashMap[String, Long]()
  var jarSet : HashMap[String, Long] = new HashMap[String, Long]()
  
  // Downloads all file dependencies from the Master file server
  def downloadDependencies(currentFileSet : HashMap[String, Long], 
    currentJarSet : HashMap[String, Long]) {

    // Fetch missing file dependencies
    fileSet.filter { case(k,v) => 
      !currentFileSet.contains(k) || currentFileSet(k) < v
    }.foreach { case (k,v) => 
      Utils.fetchFile(k, new File(System.getProperty("user.dir")))
      currentFileSet(k) = v
    }
    // Fetch missing jar dependencies
    jarSet.filter { case(k,v) => 
      !currentJarSet.contains(k) || currentJarSet(k) < v
    }.foreach { case (k,v) => 
      Utils.fetchFile(k, new File(System.getProperty("user.dir")))
      currentJarSet(k) = v
    }
    
  }
  
}

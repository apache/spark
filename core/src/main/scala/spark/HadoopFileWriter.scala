package spark

import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.RecordWriter
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ReflectionUtils

import java.text.NumberFormat
import java.io.IOException
import java.net.URI

@serializable class HadoopFileWriter[K,V] (path: String, jobID: JobID, splitID: Int) {
  val conf = new JobConf()
  var taID: TaskAttemptID = null  
  var fs: FileSystem = null
  var outputName: String = null
  var outputPath: Path = null
  var tempOutputPath: Path = null
  var tempTaskOutputPath: Path = null
  var writer: RecordWriter[K,V] = null
  
  def open() {
    if (jobID == null)
      throw new IllegalArgumentException("JobID is null")
    taID = new TaskAttemptID(new TaskID(jobID, true, splitID), 0)
    
    if (path == null)
      throw new IllegalArgumentException("Output path is null")
    outputPath = new Path(path)
    fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null)
      throw new IllegalArgumentException("Incorrectly formatted output path")
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)
    
    outputName = "part-"  + numfmt.format(splitID)
    outputPath = outputPath.makeQualified(fs)
    tempOutputPath = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME)
    tempTaskOutputPath = new Path(tempOutputPath, "_" + taID.toString)
    
    createDir(tempOutputPath)

    FileOutputFormat.setOutputPath(conf, outputPath)
    conf.set("mapred.task.id", taID.toString) 

    val fmt = ReflectionUtils.newInstance(classOf[TextOutputFormat[K,V]].asInstanceOf[Class[_]], conf).asInstanceOf[OutputFormat[K, V]]
    writer = fmt.getRecordWriter(fs, conf, outputName, null)
  }


  def write(key: K, value: V) {
    if (writer!=null) 
      writer.write(key, value)
    else 
      throw new IOException("HadoopFileWriter has not been opened yet.")
  }

  def close() {
    writer.close(null)
  }

  def verify(): Boolean = {
    return true
  }

  def commit(): Boolean = {
    if (moveFile(tempTaskOutputPath))
      return true
    else 
      return false
  }

  def abort():Boolean = {
    fs.delete (tempTaskOutputPath)
  }

  def createDir(dir: Path) {
    if (!fs.exists(dir)) 
      fs.mkdirs(dir)
  }

  def deleteFile(dir: Path) {
    if (fs.exists(dir)) 
      fs.delete(dir, true)
  }

  def moveFile(pathToMove: Path): Boolean = {
    var result = false
    if (fs.isFile(pathToMove)) {
      
      val finalPath = getFinalPath(pathToMove)
      if (!fs.rename(pathToMove, finalPath)) {
        if (!fs.delete(finalPath)) {
          throw new IOException("Failed to delete earlier output of task: " + taID)
        }
        if (!fs.rename(pathToMove, finalPath)) {
          throw new IOException("Failed to save output of task: "+ taID)
        }
      }
      println ("Moved '"+ pathToMove +"' to '"+ finalPath+"'")
      result = true
    } else if (fs.getFileStatus(pathToMove).isDir) {
      val paths = fs.listStatus(pathToMove)
      val finalPath = getFinalPath(pathToMove)
      createDir(finalPath)
      result = true
      if (paths != null)
        paths.foreach(path => if (!moveFile(path.getPath())) result = false)
    }
    return result
  }

  def getFinalPath (pathToMove: Path): Path = {
    val pathToMoveUri = pathToMove.toUri
    val relPathToMoveUri = tempTaskOutputPath.toUri.relativize(pathToMoveUri)
    if (relPathToMoveUri ==  pathToMoveUri) {
      throw new IOException("Could not get relative path of '"+pathToMove+"' from '"+tempTaskOutputPath+"'")
    }
    val relPathToMove = relPathToMoveUri.getPath
    if (relPathToMove.length > 0) {
      return new Path (outputPath, relPathToMove) 
    } else {
      return outputPath
    }
  }

}

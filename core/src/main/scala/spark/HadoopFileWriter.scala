package org.apache.hadoop.mapred

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ReflectionUtils

import java.text.NumberFormat
import java.io.IOException
import java.net.URI

@serializable class HadoopFileWriter[K,V] (path: String, jobID: JobID, splitID: Int) {
  val conf = new JobConf()
  var fs: FileSystem = null
  var outputName: String = null
  var outputPath: Path = null
  var tempOutputPath: Path = null
  var tempTaskOutputPath: Path = null
  var taID: TaskAttemptID = null
  var taCtxt: TaskAttemptContext = null
  var cmtr: OutputCommitter = null
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
  
    taID = new TaskAttemptID(new TaskID(jobID, true, splitID), 0)

    FileOutputFormat.setOutputPath(conf, outputPath)
    conf.set("mapred.task.id", taID.toString) 

    val jCtxt = new JobContext(conf, jobID) 
    taCtxt = new TaskAttemptContext(conf, taID)
  
    val fmt = ReflectionUtils.newInstance(classOf[TextOutputFormat[K,V]].asInstanceOf[Class[_]], conf).asInstanceOf[OutputFormat[K, V]]    
    cmtr = conf.getOutputCommitter()
    cmtr.setupJob(jCtxt) 		// creates the required directories
    cmtr.setupTask(taCtxt) 		// does nothing actually!

    writer = fmt.getRecordWriter(fs, conf, outputName, Reporter.NULL)
  }


  def write(key: K, value: V) {
    if (writer!=null) 
      writer.write(key, value)
    else 
      throw new IOException("HadoopFileWriter has not been opened yet.")
  }

  def close() {
    writer.close(Reporter.NULL)
  }

  def commit(): Boolean = {
    if (taCtxt == null) 
      throw new Exception("Task context is null")
    if (cmtr.needsTaskCommit(taCtxt)) {
      try {
        cmtr.commitTask(taCtxt)
        return true
      } catch {
        case e:IOException => { 
          println ("Error committing the output of task: " + taID) 
          e.printStackTrace()
          cmtr.abortTask(taCtxt)
        }
      }
    }
    return false
  }

  def commit(taIDStr: String): Boolean = {
    taID = TaskAttemptID.forName(taIDStr)
    conf.set("mapred.task.id", taID.toString) 
    taCtxt = new TaskAttemptContext(conf, taID)
    return commit()
  }

  def getTaskIDAsStr(): String = {
    if (taID == null) 
      throw new Exception("Task ID is null")
    return taID.toString
  }
}

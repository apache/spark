package org.apache.hadoop.mapred

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
/*import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.FileOutputCommitter
*/

import java.text.SimpleDateFormat
import java.text.NumberFormat
import java.io.IOException
import java.net.URI
import java.util.Date

import spark.SerializableWritable

@serializable
class HadoopTextFileWriter (path: String)
extends HadoopFileWriter [NullWritable, Text, TextOutputFormat[NullWritable, Text], FileOutputCommitter] (path) {
  def write (str: String) {
    write(null.asInstanceOf[NullWritable], new Text(str))
  }
}


@serializable 
class HadoopFileWriter [K, V, F <: OutputFormat[K,V], C <: OutputCommitter] 
  (path: String, @transient jobConf: JobConf) 
  (implicit fm: ClassManifest[F], cm: ClassManifest[C]) {  
  
  private val now = new Date() 
  private val conf = new SerializableWritable[JobConf](if (jobConf == null) new JobConf() else  jobConf)
  private val confProvided = (jobConf != null)
  
  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: RecordWriter[K,V] = null
  @transient private var format: OutputFormat[K,V] = null.asInstanceOf
  @transient private var committer: OutputCommitter = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null

  def this (path: String)(implicit fm: ClassManifest[F], cm: ClassManifest[C]) = this(path, null)

  def preSetup() {
    setIDs(0, 0, 0)
    setConfParams()
    
    val jCtxt = getJobContext() 
    getOutputCommitter().setupJob(jCtxt) 		
  }


  def setup(jobid: Int, splitid: Int, attemptid: Int) {
    setIDs(jobid, splitid, attemptid)
    setConfParams() 
  }

  def open() {
    val numfmt = NumberFormat.getInstance()
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)
    
    val outputName = "part-"  + numfmt.format(splitID)
    val fs = HadoopFileWriter.createPathFromString(path, conf.value).getFileSystem(conf.value)

    getOutputCommitter().setupTask(getTaskContext()) 
    writer = getOutputFormat().getRecordWriter(fs, conf.value, outputName, Reporter.NULL)
  }

  def write(key: K, value: V) {
    if (writer!=null) 
      writer.write(key, value)
    else 
      throw new IOException("Writer is null, open() has not been called")
  }

  def close() {
    writer.close(Reporter.NULL)
  }

  def commit(): Boolean = {
    var result = false
    val taCtxt = getTaskContext()
    val cmtr = getOutputCommitter() 
    if (cmtr.needsTaskCommit(taCtxt)) {
      println (taID + ": Commit required")
      try {
        cmtr.commitTask(taCtxt)
        println (taID + ": Committed")
        result = true
      } catch {
        case e:IOException => { 
          println ("Error committing the output of task: " + taID.value) 
          e.printStackTrace()
          cmtr.abortTask(taCtxt)
        }
      } finally {
        println ("Cleaning up")
      }
      println ("Returning result = " + result)
      return result
    } 
    println ("No need to commit output of task: " + taID.value)
    return true
  }

  def cleanup() {
    getOutputCommitter().cleanupJob(getJobContext())
  }

  // ********* Private Functions *********

  private def getOutputFormat(): OutputFormat[K, V] = {
    if (format == null) format = conf.value.getOutputFormat().asInstanceOf[OutputFormat[K,V]]
    //format = ReflectionUtils.newInstance(fm.erasure.asInstanceOf[Class[_]], conf.value).asInstanceOf[OutputFormat[K,V]]    
    return format 
  }

  private def getOutputCommitter(): OutputCommitter = {
    if (committer == null) committer = conf.value.getOutputCommitter().asInstanceOf[OutputCommitter]
    return committer
  }

  private def getJobContext(): JobContext = {
    if (jobContext == null) jobContext = new JobContext(conf.value, jID.value)   
    return jobContext
  }

  private def getTaskContext(): TaskAttemptContext = {
    if (taskContext == null) taskContext =  new TaskAttemptContext(conf.value, taID.value)
    return taskContext
  }

  private def setIDs(jobid: Int, splitid: Int, attemptid: Int) {
    jobID = jobid
    splitID = splitid
    attemptID = attemptid

    jID = new SerializableWritable[JobID](HadoopFileWriter.createJobID(now, jobid))
    taID = new SerializableWritable[TaskAttemptID] (new TaskAttemptID(new TaskID(jID.value, true, splitID), attemptID))
  }

  private def setConfParams() {
    if (!confProvided) {
      conf.value.setOutputFormat(fm.erasure.asInstanceOf[Class[F]])  
      conf.value.setOutputCommitter(cm.erasure.asInstanceOf[Class[C]])
    }
     
    FileOutputFormat.setOutputPath(conf.value, HadoopFileWriter.createPathFromString(path, conf.value))
    
    conf.value.set("mapred.job.id", jID.value.toString);
    conf.value.set("mapred.tip.id", taID.value.getTaskID.toString); 
    conf.value.set("mapred.task.id", taID.value.toString);
    conf.value.setBoolean("mapred.task.is.map", true);
    conf.value.setInt("mapred.task.partition", splitID);
  }
}

object HadoopFileWriter {
  def createJobID(time: Date, id: Int): JobID = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    return new JobID(jobtrackerID, id)
  }
  
  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null)
      throw new IllegalArgumentException("Output path is null")
    var outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null)
      throw new IllegalArgumentException("Incorrectly formatted output path")
    outputPath = outputPath.makeQualified(fs)
    return outputPath
  }
}

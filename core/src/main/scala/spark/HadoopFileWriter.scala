package org.apache.hadoop.mapred

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text

import java.text.SimpleDateFormat
import java.text.NumberFormat
import java.io.IOException
import java.net.URI
import java.util.Date

import spark.SerializableWritable
import spark.Logging

@serializable 
class HadoopFileWriter (path: String,
                        keyClass: Class[_],
                        valueClass: Class[_],
                        outputFormatClass: Class[_ <: OutputFormat[AnyRef,AnyRef]],
                        outputCommitterClass: Class[_ <: OutputCommitter],
                        @transient jobConf: JobConf = null) extends Logging {  
  
  private val now = new Date()
  private val conf = new SerializableWritable[JobConf](if (jobConf == null) new JobConf() else  jobConf)
  private val confProvided = (jobConf != null)
  
  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: RecordWriter[AnyRef,AnyRef] = null
  @transient private var format: OutputFormat[AnyRef,AnyRef] = null
  @transient private var committer: OutputCommitter = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null
  
  def this (path: String, @transient jobConf: JobConf) 
            = this (path, 
                    jobConf.getOutputKeyClass, 
                    jobConf.getOutputValueClass, 
                    jobConf.getOutputFormat().getClass.asInstanceOf[Class[OutputFormat[AnyRef,AnyRef]]], 
                    jobConf.getOutputCommitter().getClass.asInstanceOf[Class[OutputCommitter]], 
                    jobConf)
  
  def this (path: String, 
            keyClass: Class[_],
            valueClass: Class[_],
            outputFormatClass: Class[_ <: OutputFormat[AnyRef,AnyRef]],
            outputCommitterClass: Class[_ <: OutputCommitter])
            
            = this (path, 
                    keyClass, 
                    valueClass, 
                    outputFormatClass, 
                    outputCommitterClass,
                    null)

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
    val fs = HadoopFileWriter.createPathFromString(path, conf.value)
                             .getFileSystem(conf.value)

    getOutputCommitter().setupTask(getTaskContext()) 
    writer = getOutputFormat().getRecordWriter(fs, conf.value, outputName, Reporter.NULL)
  }

  def write(key: AnyRef, value: AnyRef) {
    if (writer!=null) {
      //println (">>> Writing ("+key.toString+": " + key.getClass.toString + ", " + value.toString + ": " + value.getClass.toString + ")")
      writer.write(key, value)
    } else 
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
      try {
        cmtr.commitTask(taCtxt)
        logInfo (taID + ": Committed")
        result = true
      } catch {
        case e:IOException => { 
          logError ("Error committing the output of task: " + taID.value) 
          e.printStackTrace()
          cmtr.abortTask(taCtxt)
        }
      }   
      return result
    } 
    logWarning ("No need to commit output of task: " + taID.value)
    return true
  }

  def cleanup() {
    getOutputCommitter().cleanupJob(getJobContext())
  }

  // ********* Private Functions *********

  private def getOutputFormat(): OutputFormat[AnyRef,AnyRef] = {
    if (format == null) 
      format = conf.value.getOutputFormat().asInstanceOf[OutputFormat[AnyRef,AnyRef]]
    return format 
  }

  private def getOutputCommitter(): OutputCommitter = {
    if (committer == null) 
      committer = conf.value.getOutputCommitter().asInstanceOf[OutputCommitter]
    return committer
  }

  private def getJobContext(): JobContext = {
    if (jobContext == null) 
      jobContext = new JobContext(conf.value, jID.value)   
    return jobContext
  }

  private def getTaskContext(): TaskAttemptContext = {
    if (taskContext == null) 
      taskContext =  new TaskAttemptContext(conf.value, taID.value)
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
      conf.value.setOutputFormat(outputFormatClass)  
      conf.value.setOutputCommitter(outputCommitterClass)
      conf.value.setOutputKeyClass(keyClass)
      conf.value.setOutputValueClass(valueClass)
    } else {
      
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

  def getInstance[K, V, F <: OutputFormat[K,V], C <: OutputCommitter](path: String)
    (implicit km: ClassManifest[K], vm: ClassManifest[V], fm: ClassManifest[F], cm: ClassManifest[C]): HadoopFileWriter = {
    new HadoopFileWriter(path, km.erasure, vm.erasure, fm.erasure.asInstanceOf[Class[OutputFormat[AnyRef,AnyRef]]], cm.erasure.asInstanceOf[Class[OutputCommitter]])
  }
}
